import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

const config: Config = {
  title: 'SATIF',
  tagline: 'Any to Any Files Transformation',
  favicon: 'img/favicon.ico',

  // Set the production url of your site here
  url: 'https://syncpulse-solutions.github.io',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/satif/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'syncpulse-solutions', // Usually your GitHub org/user name.
  projectName: 'satif', // Usually your repo name.
  deploymentBranch: 'gh-pages',
  trailingSlash: false,

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  markdown: {
    mermaid: true,
    format: 'detect',
  },
  themes: ['@docusaurus/theme-mermaid'],

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            'https://github.com/syncpulse-solutions/satif/tree/main/docs/',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/satif-logo.png',
    navbar: {
      title: 'SATIF',
      logo: {
        alt: 'SATIF Logo',
        src: 'img/satif-logo.png',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'tutorialSidebar',
          position: 'left',
          label: 'Docs',
        },
        {
          href: 'https://github.com/syncpulse-solutions/satif',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {
              label: 'Getting Started',
              to: '/docs/overview',
            },
            {
              label: 'Quickstart',
              to: '/docs/quickstart',
            },
            {
              label: 'Core Concepts',
              to: '/docs/concepts/sdif',
            },
          ],
        },
        {
          title: 'Resources',
          items: [
            {
              label: 'API Reference',
              to: '/docs/api_reference',
            },
            {
              label: 'Standardizers',
              to: '/docs/standardizers',
            },
            {
              label: 'Transformers',
              to: '/docs/transformers',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/syncpulse-solutions/satif',
            },
            {
              label: 'Issues',
              href: 'https://github.com/syncpulse-solutions/satif/issues',
            },
            {
              label: 'Discussions',
              href: 'https://github.com/syncpulse-solutions/satif/discussions',
            },
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'Syncpulse.fr',
              href: 'https://syncpulse.fr',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Syncpulse. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
    },
    mermaid: {
      theme: {light: 'neutral', dark: 'dark'},
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
