import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import type {ReactNode} from 'react';

import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <h1 className="hero__title">{siteConfig.title}</h1>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/overview">
            Get Started - 5min ⏱️
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={`${siteConfig.title}`}
      description="AI toolkit for simplifying and automating the transformation of ANY input files into ANY output files">
      <HomepageHeader />
      <main>
        <HomepageFeatures />
        <section className={styles.ctaSection}>
          <div className="container">
            <div className={styles.ctaContent}>
              <h2>Ready to Transform Your Data?</h2>
              <p>
                Join developers who are already using SATIF to automate their file transformation workflows.
                Start with our quickstart guide and transform your first files in minutes.
              </p>
              <div className={styles.ctaButtons}>
                <Link
                  className="button button--primary button--lg"
                  to="/docs/quickstart">
                  Start Building
                </Link>
                <Link
                  className="button button--secondary button--lg"
                  to="/docs/overview">
                  Learn More
                </Link>
              </div>
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}
