pub(crate) mod pipeline {
    use crate::config::common::sink::SinkConfig;
    use crate::config::common::source::{SourceConfig, SourceType};
    use crate::config::common::transformer::TransformerConfig;
    use crate::Result;

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct PipelineConfig {
        pub(crate) buffer_reader_config: BufferReaderConfig,
        pub(crate) buffer_writer_config: BufferWriterConfig,
        pub(crate) vertex_config: VertexConfig,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct SourceVtxConfig {
        pub(crate) source_config: SourceConfig,
        pub(crate) transformer_config: Option<TransformerConfig>,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct SinkVtxConfig {
        pub(crate) sink_config: SinkConfig,
        pub(crate) fb_sink_config: Option<SinkConfig>,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum VertexConfig {
        Source(SourceVtxConfig),
        Sink(SinkVtxConfig),
    }

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct BufferReaderConfig {}

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct BufferWriterConfig {}

    impl PipelineConfig {
        pub fn load(_pipeline_spec_obj: String) -> Result<Self> {
            Ok(PipelineConfig {
                buffer_reader_config: BufferReaderConfig {},
                buffer_writer_config: BufferWriterConfig {},
                vertex_config: VertexConfig::Source(SourceVtxConfig {
                    source_config: SourceConfig {
                        source_type: SourceType::Generator(Default::default()),
                    },
                    transformer_config: None,
                }),
            })
        }
    }

    pub(crate) mod ISB {}
}
