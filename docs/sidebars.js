/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    {type: 'doc', id: 'README', label: 'Home'},
    {
      type: 'category',
      label: 'Getting Started',
      items: [
        {type: 'doc', id: 'quick-start', label: 'Overview'},
        {
          type: 'doc',
          id: 'getting-started/prerequisites-and-installation',
          label: 'Prerequisites & Installation',
        },
        {type: 'doc', id: 'getting-started/monovertex', label: 'MonoVertex'},
        {type: 'doc', id: 'getting-started/pipeline', label: 'Pipeline'},
        {type: 'doc', id: 'getting-started/whats-next', label: "What's Next"},
      ],
    },
    {
      type: 'category',
      label: 'User Guide',
      items: [
        {
          type: 'category',
          label: 'Core Concepts',
          items: [
            {type: 'doc', id: 'core-concepts/overview', label: 'Overview'},
            'core-concepts/pipeline',
            'core-concepts/monovertex',
            'core-concepts/serving',
            'core-concepts/vertex',
            'core-concepts/inter-step-buffer',
            'core-concepts/inter-step-buffer-service',
            {
              type: 'doc',
              id: 'core-concepts/streaming',
              label: 'Streaming Architecture',
            },
            'core-concepts/watermarks',
          ],
        },
        {
          type: 'category',
          label: 'Sources',
          items: [
            {type: 'doc', id: 'user-guide/sources/overview', label: 'Overview'},
            'user-guide/sources/generator',
            'user-guide/sources/http',
            'user-guide/sources/kafka',
            'user-guide/sources/pulsar',
            'user-guide/sources/nats',
            'user-guide/sources/jetstream',
            {type: 'doc', id: 'user-guide/sources/sqs', label: 'SQS Source'},
            'user-guide/sources/user-defined-sources',
            {
              type: 'category',
              label: 'Data Transformer',
              items: [
                {
                  type: 'doc',
                  id: 'user-guide/sources/transformer/overview',
                  label: 'Overview',
                },
                {
                  type: 'category',
                  label: 'Built-in Transformers',
                  items: [
                    {
                      type: 'doc',
                      id: 'user-guide/sources/transformer/builtin-transformers/README',
                      label: 'Overview',
                    },
                    {
                      type: 'doc',
                      id: 'user-guide/sources/transformer/builtin-transformers/filter',
                      label: 'Filter',
                    },
                    {
                      type: 'doc',
                      id: 'user-guide/sources/transformer/builtin-transformers/event-time-extractor',
                      label: 'Event Time Extractor',
                    },
                    {
                      type: 'doc',
                      id: 'user-guide/sources/transformer/builtin-transformers/time-extraction-filter',
                      label: 'Event Time Extraction Filter',
                    },
                  ],
                },
              ],
            },
          ],
        },
        {
          type: 'category',
          label: 'Sinks',
          items: [
            {type: 'doc', id: 'user-guide/sinks/overview', label: 'Overview'},
            'user-guide/sinks/kafka',
            'user-guide/sinks/log',
            'user-guide/sinks/blackhole',
            'user-guide/sinks/sqs',
            {
              type: 'doc',
              id: 'user-guide/sinks/user-defined-sinks',
              label: 'User-defined Sinks',
            },
            {
              type: 'doc',
              id: 'user-guide/sinks/fallback',
              label: 'Fallback Sink',
            },
            {
              type: 'doc',
              id: 'user-guide/sinks/on-success',
              label: 'OnSuccess Sink',
            },
            {
              type: 'doc',
              id: 'user-guide/sinks/retry-strategy',
              label: 'Retry Strategy',
            },
          ],
        },
        {
          type: 'category',
          label: 'User-defined Functions',
          items: [
            {
              type: 'doc',
              id: 'user-guide/user-defined-functions/user-defined-functions',
              label: 'Overview',
            },
            {
              type: 'category',
              label: 'Map',
              items: [
                {
                  type: 'doc',
                  id: 'user-guide/user-defined-functions/map/map',
                  label: 'Overview',
                },
                {
                  type: 'category',
                  label: 'Built-in UDFs',
                  items: [
                    {
                      type: 'doc',
                      id: 'user-guide/user-defined-functions/map/builtin-functions/README',
                      label: 'Overview',
                    },
                    {
                      type: 'doc',
                      id: 'user-guide/user-defined-functions/map/builtin-functions/cat',
                      label: 'Cat',
                    },
                    {
                      type: 'doc',
                      id: 'user-guide/user-defined-functions/map/builtin-functions/filter',
                      label: 'Filter',
                    },
                  ],
                },
                {
                  type: 'doc',
                  id: 'user-guide/user-defined-functions/map/examples',
                  label: 'Examples',
                },
              ],
            },
            {
              type: 'category',
              label: 'Reduce',
              items: [
                {
                  type: 'doc',
                  id: 'user-guide/user-defined-functions/reduce/reduce',
                  label: 'Overview',
                },
                {
                  type: 'category',
                  label: 'Windowing',
                  items: [
                    {
                      type: 'doc',
                      id: 'user-guide/user-defined-functions/reduce/windowing/windowing',
                      label: 'Overview',
                    },
                    {
                      type: 'doc',
                      id: 'user-guide/user-defined-functions/reduce/windowing/fixed',
                      label: 'Fixed',
                    },
                    {
                      type: 'doc',
                      id: 'user-guide/user-defined-functions/reduce/windowing/sliding',
                      label: 'Sliding',
                    },
                    {
                      type: 'doc',
                      id: 'user-guide/user-defined-functions/reduce/windowing/session',
                      label: 'Session',
                    },
                    {
                      type: 'doc',
                      id: 'user-guide/user-defined-functions/reduce/windowing/accumulator',
                      label: 'Accumulator',
                    },
                  ],
                },
                {
                  type: 'doc',
                  id: 'user-guide/user-defined-functions/reduce/examples',
                  label: 'Examples',
                },
              ],
            },
          ],
        },
        {
          type: 'category',
          label: 'SDKs',
          items: [
            {type: 'doc', id: 'user-guide/sdks/overview', label: 'Overview'},
            'user-guide/sdks/compatibility',
            'user-guide/sdks/features',
          ],
        },
        {
          type: 'category',
          label: 'Reference',
          items: [
            {type: 'doc', id: 'core-concepts/message-headers', label: 'Message Headers'},
            {type: 'doc', id: 'user-guide/reference/tracing', label: 'Tracing'},
            'user-guide/reference/pipeline-tuning',
            'user-guide/reference/autoscaling',
            'user-guide/reference/conditional-forwarding',
            'user-guide/reference/pipeline-operations',
            'user-guide/reference/gpu',
            'user-guide/reference/join-vertex',
            'user-guide/reference/multi-partition',
            'user-guide/reference/ordered-processing',
            'user-guide/reference/side-inputs',
            'user-guide/reference/mvtx-tuning',
            'user-guide/reference/mvtx-operations',
            {
              type: 'doc',
              id: 'user-guide/reference/mvtx-streaming',
              label: 'MonoVertex Streaming Mode',
            },
            'user-guide/reference/distributed-throttling',
            'user-guide/reference/monovertex-bypass',
            {
              type: 'doc',
              id: 'user-guide/reference/per-message-nack',
              label: 'Per-message Nack',
            },
            {
              type: 'doc',
              id: 'user-guide/reference/memory-profiling',
              label: 'Memory profiling',
            },
            {
              type: 'category',
              label: 'Configuration',
              items: [
                'user-guide/reference/configuration/pod-specifications',
                'user-guide/reference/configuration/container-resources',
                'user-guide/reference/configuration/volumes',
                'user-guide/reference/configuration/environment-variables',
                'user-guide/reference/configuration/labels-and-annotations',
                'user-guide/reference/configuration/init-containers',
                'user-guide/reference/configuration/sidecar-containers',
                'user-guide/reference/configuration/liveness-and-readiness',
                'user-guide/reference/configuration/pipeline-customization',
                'user-guide/reference/configuration/dra',
                'user-guide/reference/configuration/istio',
                'user-guide/reference/configuration/max-message-size',
                'user-guide/reference/configuration/update-strategy',
              ],
            },
            'user-guide/reference/kustomize/kustomize',
            {type: 'link', label: 'APIs', href: '/APIs/'},
          ],
        },
        {
          type: 'category',
          label: 'Use Cases',
          items: [
            'user-guide/use-cases/overview',
            'user-guide/use-cases/monitoring-and-observability',
          ],
        },
        {
          type: 'category',
          label: 'UI',
          items: [
            {type: 'doc', id: 'user-guide/UI/overview', label: 'Overview'},
            {type: 'doc', id: 'user-guide/UI/errors', label: 'Errors'},
            {type: 'doc', id: 'user-guide/UI/logs', label: 'Logs'},
            {type: 'doc', id: 'user-guide/UI/metrics-tab', label: 'Metrics'},
            {type: 'doc', id: 'user-guide/UI/pods-view', label: 'Pods View'},
          ],
        },
        {type: 'doc', id: 'user-guide/FAQ', label: 'FAQs'},
      ],
    },
    {
      type: 'category',
      label: 'Operator Manual',
      items: [
        {type: 'doc', id: 'operations/releases', label: 'Releases'},
        'operations/installation',
        {
          type: 'doc',
          id: 'operations/validating-webhook',
          label: 'Validating Webhook',
        },
        {
          type: 'category',
          label: 'Configuration',
          items: [
            {
              type: 'doc',
              id: 'operations/controller-configmap',
              label: 'Controller Configuration',
            },
            {
              type: 'category',
              label: 'UI Server',
              items: [
                {
                  type: 'doc',
                  id: 'operations/ui/ui-access-path',
                  label: 'Access Path',
                },
                {
                  type: 'category',
                  label: 'Authentication',
                  items: [
                    {
                      type: 'doc',
                      id: 'operations/ui/authn/authentication',
                      label: 'Overview',
                    },
                    {
                      type: 'doc',
                      id: 'operations/ui/authn/dex',
                      label: 'SSO with Dex',
                    },
                    {
                      type: 'doc',
                      id: 'operations/ui/authn/local-users',
                      label: 'Local Users',
                    },
                  ],
                },
                {
                  type: 'doc',
                  id: 'operations/ui/authz/rbac',
                  label: 'Authorization',
                },
              ],
            },
            'operations/metrics/metrics',
            'operations/grafana',
          ],
        },
        {type: 'doc', id: 'operations/security', label: 'Security'},
      ],
    },
    {
      type: 'category',
      label: 'Contributor Guide',
      items: [
        'development/development',
        {
          type: 'category',
          label: 'Specifications',
          items: [
            {
              type: 'doc',
              id: 'specifications/overview',
              label: 'Overview',
            },
            'specifications/controllers',
            'specifications/autoscaling',
            {
              type: 'doc',
              id: 'specifications/edges-buffers-buckets',
              label: 'Edges, Buffers and Buckets',
            },
            {
              type: 'doc',
              id: 'specifications/side-inputs',
              label: 'Side Inputs',
            },
            {
              type: 'doc',
              id: 'specifications/authorization',
              label: 'UI Authorization',
            },
          ],
        },
        'development/debugging',
        'development/static-code-analysis',
        'development/releasing',
      ],
    },
    {type: 'link', label: 'Numaproj', href: 'https://numaproj.io'},
  ],
};

module.exports = sidebars;
