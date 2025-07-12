import logging
from typing import Any, Dict, List, Optional, Tuple, Union

from deepdiff import DeepDiff
from sdif_db.schema import SDIFSchemaConfig, apply_rules_to_schema

log = logging.getLogger(__name__)


class SDIFSchemaComparator:
    """
    Compares two SDIF structural schemas based on a flexible configuration.
    Provides methods for checking equivalence and compatibility (subset relationship).
    The input schemas are expected to be the direct output of SDIFDatabase.get_schema().
    """

    def __init__(self, config: Optional[SDIFSchemaConfig] = None):
        """
        Initializes the comparator with a specific configuration.

        Args:
            config: An SDIFSchemaConfig instance. If None, a default config is used.
        """
        self.config = config if config else SDIFSchemaConfig()

    def compare(
        self,
        schema1: Dict[str, Any],
        schema2: Dict[str, Any],
        verbose_diff_level: int = 0,
    ) -> Tuple[bool, Union[List[str], Dict[str, Any]]]:
        """
        Compares two structural SDIF schemas.

        Args:
            schema1: The first structural schema (output of SDIFDatabase.get_schema()).
            schema2: The second structural schema.
            verbose_diff_level: Controls verbosity of the difference report.
                0: Returns a summarized list of human-readable differences.
                1: Returns the DeepDiff object as a dictionary.
                2 (or more): Returns the full DeepDiff object (can be large).

        Returns:
            A tuple: (are_equivalent: bool, differences: Union[List[str], Dict[str, Any]]).
                     'differences' depends on verbose_diff_level.
        """
        log.debug("Applying rules to schema 1...")
        minimal_schema1 = apply_rules_to_schema(schema1, self.config)
        log.debug("Applying rules to schema 2...")
        minimal_schema2 = apply_rules_to_schema(schema2, self.config)

        log.debug("Comparing minimal schemas...")
        # ignore_order=False because canonicalization should handle order where specified by config.
        # report_repetition=True can be useful for complex list diffs.
        diff = DeepDiff(
            minimal_schema1,
            minimal_schema2,
            ignore_order=False,  # Our canonical form handles order based on config
            report_repetition=True,
            verbose_level=2,  # Get full details for potential custom summary
        )

        are_equivalent = not bool(diff)

        if verbose_diff_level >= 2:
            return are_equivalent, diff
        if verbose_diff_level == 1:
            return are_equivalent, diff.to_dict() if diff else {}

        # Format differences into a human-readable list (verbose_diff_level == 0)
        diff_summary: List[str] = []
        if not are_equivalent:
            diff_summary.append("Schema differences found based on configuration:")
            for diff_type, changes in diff.items():
                if not changes:
                    continue  # Skip empty change sets

                if diff_type == "dictionary_item_added":
                    for item in changes:  # item is a DiffLevel object
                        diff_summary.append(f"  + Added at '{item.path()}': {item.t2}")
                elif diff_type == "dictionary_item_removed":
                    for item in changes:  # item is a DiffLevel object
                        diff_summary.append(
                            f"  - Removed at '{item.path()}': {item.t1}"
                        )
                elif diff_type == "values_changed":
                    for item_path_str, change_details in changes.items():
                        diff_summary.append(
                            f"  ~ Changed at '{item_path_str}': from "
                            f"'{change_details['old_value']}' to '{change_details['new_value']}'"
                        )
                elif diff_type == "type_changes":
                    for item_path_str, change_details in changes.items():
                        diff_summary.append(
                            f"  ! Type changed at '{item_path_str}': from "
                            f"{change_details['old_type']} to {change_details['new_type']}"
                        )
                elif diff_type == "iterable_item_added":
                    for item_path_str, item_value in changes.items():
                        diff_summary.append(
                            f"  + Item added to iterable at '{item_path_str}': {item_value}"
                        )
                elif diff_type == "iterable_item_removed":
                    for item_path_str, item_value in changes.items():
                        diff_summary.append(
                            f"  - Item removed from iterable at '{item_path_str}': {item_value}"
                        )
                else:
                    # Generic fallback for other diff types DeepDiff might produce
                    diff_summary.append(f"  * Other difference type '{diff_type}':")
                    try:
                        # Attempt a brief summary of the changes for this unknown type
                        change_str = str(changes)
                        max_len = 150
                        summary = (
                            change_str[:max_len] + "..."
                            if len(change_str) > max_len
                            else change_str
                        )
                        diff_summary.append(f"    {summary}")
                    except Exception:
                        diff_summary.append(
                            "    Details unavailable for this difference type."
                        )

            if not diff_summary[
                1:
            ]:  # Only contains the header "Schema differences found..."
                diff_summary.append(
                    "    Specific differences were found, but summary generation did not capture details. Consider verbose_diff_level >= 1."
                )

        else:  # are_equivalent is True
            diff_summary.append(
                "Schemas are equivalent based on the current configuration."
            )

        return are_equivalent, diff_summary

    def is_compatible_with(
        self,
        consumer_schema: Dict[str, Any],
        producer_schema: Dict[str, Any],
    ) -> bool:
        """
        Checks if the producer_schema is structurally compatible with the consumer_schema,
        based on the requirements defined in the comparator's configuration (self.config).

        Compatibility means the producer_schema provides at least all the structural
        elements and guarantees required by the consumer_schema according to the config.
        The producer_schema can have additional elements not required by the consumer.

        Args:
            consumer_schema: The schema defining the requirements (consumer's view).
            producer_schema: The schema being checked for compliance (producer's actual schema).

        Returns:
            True if producer_schema is compatible with consumer_schema's requirements,
            False otherwise.
        """
        log.debug(
            "Applying consumer rules (from config) to consumer schema for compatibility check..."
        )
        min_consumer_schema = apply_rules_to_schema(consumer_schema, self.config)
        log.debug(
            "Applying consumer rules (from config) to producer schema for compatibility check..."
        )
        min_producer_schema_viewed_by_consumer = apply_rules_to_schema(
            producer_schema, self.config
        )

        log.debug("Checking recursive compatibility...")
        return self._check_compatibility_recursive(
            min_consumer_schema, min_producer_schema_viewed_by_consumer
        )

    def _check_compatibility_recursive(
        self, consumer_part: Any, producer_part: Any
    ) -> bool:
        """Recursive helper for is_compatible_with."""
        # If consumer part is None (e.g., optional section not present or ignored by config),
        # it imposes no requirement.
        if consumer_part is None:
            return True

        # If consumer expects something, but producer provides None (e.g., section missing)
        # then it's incompatible, unless consumer also expected None.
        if producer_part is None and consumer_part is not None:
            return False

        consumer_type = type(consumer_part)
        producer_type = type(producer_part)

        # Types must generally match for compatibility, except when consumer is None (handled above)
        if consumer_type != producer_type:
            log.debug(
                f"Type mismatch: consumer expected {consumer_type}, producer has {producer_type}"
            )
            return False

        # --- Comparison based on type ---

        if isinstance(
            consumer_part, (str, int, float, bool, bytes)
        ):  # Basic immutable types
            return consumer_part == producer_part

        elif isinstance(consumer_part, dict):
            # Check if all keys required by consumer exist in producer
            for key, consumer_value in consumer_part.items():
                if key not in producer_part:
                    log.debug(f"Missing key in producer dict: '{key}'")
                    return False
                # Recursively check compatibility for the value
                if not self._check_compatibility_recursive(
                    consumer_value, producer_part[key]
                ):
                    log.debug(f"Incompatible value for key: '{key}'")
                    return False
            # All required keys exist and their values are compatible
            return True

        elif isinstance(consumer_part, tuple):
            # Represents an ordered list in the minimal schema (e.g., columns, pk columns)
            # For compatibility, the producer must contain all items required by the consumer,
            # potentially more. Using set subset check handles this.
            # Note: This assumes elements within the tuple are hashable due to _canonicalize_value
            try:
                consumer_set = set(consumer_part)
                producer_set = set(producer_part)
                if not consumer_set.issubset(producer_set):
                    log.debug(
                        f"Producer tuple/list missing required items. Consumer needs: {consumer_set - producer_set}"
                    )
                    return False
                return True
            except TypeError:
                log.warning(
                    "Could not perform set comparison on tuple elements - likely unhashable. Falling back to element-wise check (requires exact match and order).",
                    exc_info=True,
                )
                # Fallback for safety, although canonicalization should prevent this.
                # This fallback implies producer must exactly match consumer tuple.
                return consumer_part == producer_part

        elif isinstance(consumer_part, frozenset):
            # Represents an unordered set in the minimal schema (e.g., tables_set, link_types)
            if not consumer_part.issubset(producer_part):
                log.debug(
                    f"Producer set missing required items. Consumer needs: {consumer_part - producer_part}"
                )
                return False
            return True

        else:
            # Fallback for any other types: require exact equality
            log.warning(
                f"Unhandled type in compatibility check: {consumer_type}. Falling back to equality."
            )
            return consumer_part == producer_part
