const {themes: prismThemes} = require('prism-react-renderer');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Numaflow',
  tagline: 'Data/streaming processing platform on Kubernetes',
  favicon: 'img/numaproj.svg',

  url: 'https://numaflow.numaproj.io',
  baseUrl: '/',
  organizationName: 'numaproj',
  projectName: 'numaflow',
  deploymentBranch: 'gh-pages',
  trailingSlash: true,

  onBrokenLinks: 'throw',

  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'throw',
    },
  },

  themes: [
    '@docusaurus/theme-mermaid',
    [
      require.resolve('@easyops-cn/docusaurus-search-local'),
      /** @type {import('@easyops-cn/docusaurus-search-local').PluginOptions} */
      ({
        hashed: true,
        indexDocs: true,
        indexBlog: false,
        docsRouteBasePath: '/',
        docsDir: [
          'core-concepts',
          'development',
          'getting-started',
          'operations',
          'specifications',
          'user-guide',
        ],
      }),
    ],
  ],

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          path: '.',
          routeBasePath: '/',
          exclude: [
            'APIs.md',
            'hack/**',
            'node_modules/**',
            'site/**',
            'src/**',
            'static/**',
          ],
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/numaproj/numaflow/edit/main/docs/',
        },
        blog: false,
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
        gtag: {
          trackingID: 'G-M7DD40E8RV',
          anonymizeIP: true,
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      image: 'img/numaproj.svg',
      navbar: {
        title: 'Numaflow',
        logo: {
          alt: 'Numaflow logo',
          src: 'img/numaproj.svg',
        },
        items: [
          {to: '/quick-start/', label: 'Getting Started', position: 'left'},
          {
            to: '/core-concepts/overview/',
            label: 'User Guide',
            position: 'left',
          },
          {
            to: '/operations/releases/',
            label: 'Operator Manual',
            position: 'left',
          },
          {
            to: '/development/',
            label: 'Contributor Guide',
            position: 'left',
          },
          {
            href: 'https://numaproj.io',
            label: 'Numaproj',
            position: 'right',
          },
          {
            href: 'https://github.com/numaproj/numaflow',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {label: 'Getting Started', to: '/quick-start/'},
              {label: 'User Guide', to: '/core-concepts/overview/'},
              {label: 'Operator Manual', to: '/operations/releases/'},
            ],
          },
          {
            title: 'Community',
            items: [
              {label: 'Numaproj', href: 'https://numaproj.io'},
              {
                label: 'GitHub',
                href: 'https://github.com/numaproj/numaflow',
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Numaflow Authors.`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        // Java, bash/shell/sh, and docker/dockerfile are not in Prism's default set.
        additionalLanguages: ['java', 'bash', 'docker'],
      },
    }),
};

module.exports = config;
