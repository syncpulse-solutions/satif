import type {ReactNode} from 'react';
import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  image: string;
  alt: string;
  description: ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    title: '1. Standardize to a Single SQLite DB',
    image: require('@site/static/img/inputs-to-sdif.png').default,
    alt: 'Convert any set of input files into an SDIF database',
    description: (
      <>
        Convert any set of source files (CSV, Excel, PDF, etc.) into a single,
        structured SQLite database called SDIF. SATIF handles the parsing automatically with AI.
      </>
    ),
  },
  {
    title: '2. Transform SDIF to Output Files',
    image: require('@site/static/img/sdif-to-outputs.png').default,
    alt: 'Transform SDIF into any output files',
    description: (
      <>
        Describe your target file in natural language or provide an example
        output file. SATIF's AI agent generates the Python transformation code
        for you. You can then run the code to transform the SDIF database into
        your target files.
      </>
    ),
  },
];

function Feature({title, image, alt, description}: FeatureItem) {
  return (
    <div className={clsx('col col--6')}>
      <div className="text--center">
        <img src={image} alt={alt} className={styles.featureSvg} />
      </div>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
