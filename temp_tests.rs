    #[test]
    fn test_window_config_from_group_by_fixed() {
        let window = Window {
            fixed: Some(Box::new(FixedWindow {
                length: Some(kube::core::Duration::from(Duration::from_secs(60))),
                streaming: None,
            })),
            sliding: None,
            session: None,
            accumulator: None,
        };

        let group_by = Box::new(GroupBy {
            allowed_lateness: Some(kube::core::Duration::from(Duration::from_secs(10))),
            keyed: Some(true),
            storage: None,
            window: Box::new(window),
        });

        let reducer_config = ReducerConfig::try_from(&group_by).unwrap();
        match reducer_config {
            ReducerConfig::Aligned(aligned_config) => {
                assert_eq!(
                    aligned_config.window_config.allowed_lateness,
                    Duration::from_secs(10)
                );
                assert!(aligned_config.window_config.is_keyed);

                match aligned_config.window_config.window_type {
                    AlignedWindowType::Fixed(config) => {
                        assert_eq!(config.length, Duration::from_secs(60));
                    }
                    _ => panic!("Expected fixed window type"),
                }
            }
            _ => panic!("Expected aligned reducer config"),
        }
    }

    #[test]
    fn test_window_config_from_group_by_sliding() {
        let window = Window {
            fixed: None,
            sliding: Some(Box::new(SlidingWindow {
                length: Some(kube::core::Duration::from(Duration::from_secs(60))),
                slide: Some(kube::core::Duration::from(Duration::from_secs(30))),
                streaming: None,
            })),
            session: None,
            accumulator: None,
        };

        let group_by = Box::new(GroupBy {
            allowed_lateness: None,
            keyed: None,
            storage: None,
            window: Box::new(window),
        });

        let reducer_config = ReducerConfig::try_from(&group_by).unwrap();
        match reducer_config {
            ReducerConfig::Aligned(aligned_config) => {
                assert_eq!(
                    aligned_config.window_config.allowed_lateness,
                    Duration::from_secs(0)
                );
                assert!(!aligned_config.window_config.is_keyed);

                match aligned_config.window_config.window_type {
                    AlignedWindowType::Sliding(config) => {
                        assert_eq!(config.length, Duration::from_secs(60));
                        assert_eq!(config.slide, Duration::from_secs(30));
                    }
                    _ => panic!("Expected sliding window type"),
                }
            }
            _ => panic!("Expected aligned reducer config"),
        }
    }

    #[test]
    fn test_window_config_from_group_by_session() {
        let window = Window {
            fixed: None,
            sliding: None,
            session: Some(Box::new(SessionWindow {
                timeout: Some(kube::core::Duration::from(Duration::from_secs(300))),
            })),
            accumulator: None,
        };

        let group_by = Box::new(GroupBy {
            allowed_lateness: None,
            keyed: Some(true),
            storage: None,
            window: Box::new(window),
        });

        let reducer_config = ReducerConfig::try_from(&group_by).unwrap();
        match reducer_config {
            ReducerConfig::Unaligned(unaligned_config) => {
                assert_eq!(
                    unaligned_config.window_config.allowed_lateness,
                    Duration::from_secs(0)
                );
                assert!(unaligned_config.window_config.is_keyed);

                match unaligned_config.window_config.window_type {
                    UnalignedWindowType::Session(config) => {
                        assert_eq!(config.timeout, Duration::from_secs(300));
                    }
                    _ => panic!("Expected session window type"),
                }
            }
            _ => panic!("Expected unaligned reducer config"),
        }
    }

    #[test]
    fn test_window_config_from_group_by_accumulator() {
        let window = Window {
            fixed: None,
            sliding: None,
            session: None,
            accumulator: Some(Box::new(AccumulatorWindow {
                timeout: Some(kube::core::Duration::from(Duration::from_secs(600))),
            })),
        };

        let group_by = Box::new(GroupBy {
            allowed_lateness: None,
            keyed: Some(true),
            storage: None,
            window: Box::new(window),
        });

        let reducer_config = ReducerConfig::try_from(&group_by).unwrap();
        match reducer_config {
            ReducerConfig::Unaligned(unaligned_config) => {
                assert_eq!(
                    unaligned_config.window_config.allowed_lateness,
                    Duration::from_secs(0)
                );
                assert!(unaligned_config.window_config.is_keyed);

                match unaligned_config.window_config.window_type {
                    UnalignedWindowType::Accumulator(config) => {
                        assert_eq!(config.timeout, Duration::from_secs(600));
                    }
                    _ => panic!("Expected accumulator window type"),
                }
            }
            _ => panic!("Expected unaligned reducer config"),
        }
    }

    #[test]
    fn test_window_config_from_group_by_no_window_type() {
        let window = Window {
            fixed: None,
            sliding: None,
            session: None,
            accumulator: None,
        };

        let group_by = Box::new(GroupBy {
            allowed_lateness: None,
            keyed: None,
            storage: None,
            window: Box::new(window),
        });

        let result = ReducerConfig::try_from(&group_by);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Config Error - No window type specified"
        );
    }
