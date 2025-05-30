    impl TryFrom<&Box<GroupBy>> for ReducerConfig {
        type Error = Error;
        fn try_from(group_by: &Box<GroupBy>) -> Result<Self> {
            let window = group_by.window.as_ref();
            let allowed_lateness = group_by
                .allowed_lateness
                .map_or(Duration::from_secs(0), Duration::from);
            let is_keyed = group_by.keyed.unwrap_or(false);

            if let Some(fixed) = &window.fixed {
                let window_config = AlignedWindowConfig {
                    window_type: AlignedWindowType::Fixed(fixed.clone().into()),
                    allowed_lateness,
                    is_keyed,
                };
                Ok(ReducerConfig::Aligned(AlignedReducerConfig {
                    user_defined_config: UserDefinedConfig::default(),
                    window_config,
                }))
            } else if let Some(sliding) = &window.sliding {
                let window_config = AlignedWindowConfig {
                    window_type: AlignedWindowType::Sliding(sliding.clone().into()),
                    allowed_lateness,
                    is_keyed,
                };
                Ok(ReducerConfig::Aligned(AlignedReducerConfig {
                    user_defined_config: UserDefinedConfig::default(),
                    window_config,
                }))
            } else if let Some(session) = &window.session {
                let window_config = UnalignedWindowConfig {
                    window_type: UnalignedWindowType::Session(session.clone().into()),
                    allowed_lateness,
                    is_keyed,
                };
                Ok(ReducerConfig::Unaligned(UnalignedReducerConfig {
                    reducer_type: ReducerType::UserDefined(UserDefinedConfig::session_config()),
                    window_config,
                }))
            } else if let Some(accumulator) = &window.accumulator {
                let window_config = UnalignedWindowConfig {
                    window_type: UnalignedWindowType::Accumulator(accumulator.clone().into()),
                    allowed_lateness,
                    is_keyed,
                };
                Ok(ReducerConfig::Unaligned(UnalignedReducerConfig {
                    reducer_type: ReducerType::UserDefined(UserDefinedConfig::accumulator_config()),
                    window_config,
                }))
            } else {
                Err(Error::Config("No window type specified".to_string()))
            }
        }
    }
