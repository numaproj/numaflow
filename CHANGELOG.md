# Changelog

## v1.8.0 (2026-05-28)

 * [3d8c11ff9](https://github.com/numaproj/numaflow/commit/3d8c11ff922ada1078db279c0b2272e8af59623c) Update manifests to v1.8.0
 * [51b702693](https://github.com/numaproj/numaflow/commit/51b7026934f449572bb30ae5f7055b311bb415bf) fix: remove concurrency default set by the kubebuilder  (#3445)
 * [6ee153d58](https://github.com/numaproj/numaflow/commit/6ee153d5895eacb2fad369b03a69a6b5e261d135) feat(ui): improve MonoVertex bypass visualization using `reactflow` (#3438)
 * [42722eabb](https://github.com/numaproj/numaflow/commit/42722eabb92d5202cee454d71cd3110164bedc04) feat: introduce concurrency env for udf invocation (#3317)
 * [a77bc258f](https://github.com/numaproj/numaflow/commit/a77bc258fc9994d845ef6a41bb8ded1e23a55b58) doc: document tracing configuration and span hierarchy (#3440)
 * [c33dd81e5](https://github.com/numaproj/numaflow/commit/c33dd81e592c2e33ada494390e497f8e456088cf) feat: isb spans for tracing (#3437)
 * [d8f95220a](https://github.com/numaproj/numaflow/commit/d8f95220a3795dd93f55ca86e934081384375416) feat: add spans for tracing (#3397)
 * [d7e424b6c](https://github.com/numaproj/numaflow/commit/d7e424b6caa97d5d835757b761ba139d53ac5729) fix(doc): `not` operator instead of `nor`  (#3432)
 * [3f20f8162](https://github.com/numaproj/numaflow/commit/3f20f8162e70cffb305e6093f2150c9d66953d37) fix: return with rx early when stream mapper is closed (#3431)
 * [8e07d5da3](https://github.com/numaproj/numaflow/commit/8e07d5da38aa5b06a97f6c5dcaee55b2d5db4e0a) Adding Kaseya to USERS.md (#3430)
 * [aa5d73bdd](https://github.com/numaproj/numaflow/commit/aa5d73bdde175c2642184b34a88f4c2fcb195bc6) fix: add monovtx_udf_drop_total metric for MonoVertex map UDF drops (#3427)
 * [49ce7d32c](https://github.com/numaproj/numaflow/commit/49ce7d32c19caf7aff6ab8a4f53a04d1e9c204e8) fix: prevent int32 overflow in MonoVertex autoscaler desiredReplicas (#3421)
 * [82e480a99](https://github.com/numaproj/numaflow/commit/82e480a997179c59b89e5f882077f10f9f80f0f7) fix: mark JS stream as full until proven otherwise (#3422)
 * [a9e4c600a](https://github.com/numaproj/numaflow/commit/a9e4c600acb19ec659fdcea89d3bd5ddc17cfeb1) fix: implement stronger status logic for getPipelineStatus (#3416)
 * [5d525317e](https://github.com/numaproj/numaflow/commit/5d525317e37529be7a51fafb53b58e5461dbf8ca) chore(deps): bump @babel/plugin-transform-modules-systemjs from 7.24.7 to 7.29.4 in /ui (#3420)
 * [c3dbc0bce](https://github.com/numaproj/numaflow/commit/c3dbc0bce9702ff3e4435156d64d889009ee1eee) chore(deps): bump fast-uri from 3.0.1 to 3.1.2 in /ui (#3419)
 * [e07240b79](https://github.com/numaproj/numaflow/commit/e07240b79f0e36e1584f04d4a85bdb1cf8e95e22) fix: implement `getMonoVertexStatus` to return correct health status (#3408)
 * [a70baf20a](https://github.com/numaproj/numaflow/commit/a70baf20a5e71e852b1df45c9edadafa0fa32bf4) fix: race between udf server processing and msg id insertion in map for tracking (#3401)
 * [ae92aa544](https://github.com/numaproj/numaflow/commit/ae92aa54492862b6d9e88a56ec664fc4cf7599b4) fix(doc): name reduce_pbq_write was missing the auto-appended _total suffix (#3409)
 * [85fd8bc50](https://github.com/numaproj/numaflow/commit/85fd8bc50cae3cb71ad528422de5f449c76efac7) fix: use podman as fallback when docker is not available in numaflow-… (#3407)
 * [9ec55c800](https://github.com/numaproj/numaflow/commit/9ec55c80048cfb8c6eeccae7a7d6c5e881b03384) fix: return result status to cause monovertex controller requeue (#3405)
 * [5e4873db6](https://github.com/numaproj/numaflow/commit/5e4873db6a2e28f06eaba3a67d2dcc080476c83e) fix: nack messages during token cancellation and discardLatest strategy (#3404)
 * [6b30309d9](https://github.com/numaproj/numaflow/commit/6b30309d96b3ebfcad049150ea75931ee61ce902) fix: use min event_time across in-flight msgs before publishing idle src wm (#3400)
 * [aaa5f7712](https://github.com/numaproj/numaflow/commit/aaa5f771241b1512041bcd565d99a8884a65c89c) feat: add reduce metrics for rust data plane (#3364)
 * [ce2ac174d](https://github.com/numaproj/numaflow/commit/ce2ac174d117843196d69492d6548c460970446e) fix: append parent index information in message ID offset (#3394)
 * [27e11afbe](https://github.com/numaproj/numaflow/commit/27e11afbe8e05c115fe14d1e4ff24575d352dc5f) fix: Exit with non-zero status on error and log errors when we receive duplicate message sink UDF (#3398)
 * [5cd895bca](https://github.com/numaproj/numaflow/commit/5cd895bca10217b6fe87836669ed1644a2b18793) fix: remove unnecessary scrollbars in errors tab (#3392)
 * [f2ab04d5b](https://github.com/numaproj/numaflow/commit/f2ab04d5bafeea1b8163decf882469aa1d0562d7) chore(deps): bump @xmldom/xmldom from 0.8.12 to 0.8.13 in /ui (#3393)
 * [4dc473496](https://github.com/numaproj/numaflow/commit/4dc47349642993dbbe7b9a773f8a3844d3c263cc) feat: add OTLP export into tracing setup with context propagation helpers (#3385)
 * [4ffd0b191](https://github.com/numaproj/numaflow/commit/4ffd0b19199ef8be269994f9c5c95d45909f8286) refactor: Nack Messages by default to avoid acking failed messages (#3342)
 * [06440fc5b](https://github.com/numaproj/numaflow/commit/06440fc5b042b0878c7902eb5f6809373181aab4) fix: include onSuccess in kustomize transformations (#3389)
 * [9b268e77b](https://github.com/numaproj/numaflow/commit/9b268e77b1340369c1ec59e74dafc1d9a9f7e4f4) fix: include udf in MonoVertex kustomize transformations. Closes #3322 (#3382)
 * [f5f4cc497](https://github.com/numaproj/numaflow/commit/f5f4cc4974a803cf94e32b02799a052e83b11055) fix: allow token cancellation from streaming_read errors (#3373)
 * [180059f52](https://github.com/numaproj/numaflow/commit/180059f5252737f5e840f3c54f4ed031947bb02a) chore(deps): bump github.com/moby/spdystream from 0.5.0 to 0.5.1 (#3374)
 * [50d403f19](https://github.com/numaproj/numaflow/commit/50d403f19f4fb0783df2bfb838e6cb78a6579d2b) tests: Watermark tests (#3344)
 * [2e069661c](https://github.com/numaproj/numaflow/commit/2e069661cb5d93e32f47219e59cb0b882c5af0eb) fix: Pod deletion (scale down) caused transient unhealthy vertex/mvtx (#3366)
 * [efffe7bd5](https://github.com/numaproj/numaflow/commit/efffe7bd52c0d4cd6ad1e721f8bfaea1ba4d992a) chore(deps): bump rand from 0.9.2 to 0.9.3 in /rust (#3363)
 * [8b5178381](https://github.com/numaproj/numaflow/commit/8b5178381c3ed911fffd4b5c529489e2cd2a336c) chore(deps): bump follow-redirects from 1.15.6 to 1.16.0 in /ui (#3362)
 * [6314713f1](https://github.com/numaproj/numaflow/commit/6314713f14140a2ff47490e23a27ce8feb391fe7) fix: start lastScaledAt as creationTime by default (#3358)
 * [868481d68](https://github.com/numaproj/numaflow/commit/868481d68005eebecb72556c9e8a14d2f5ed95b4) chore(deps): bump go.opentelemetry.io/otel/sdk from 1.40.0 to 1.43.0 (#3356)
 * [4ae2c3ccc](https://github.com/numaproj/numaflow/commit/4ae2c3cccf2f44441dd9f58cbd4ede3cf08d2825) doc: Update in-built kafka source documentation (#3354)
 * [81190903c](https://github.com/numaproj/numaflow/commit/81190903c722cf895b93475af48d7155dbca1c7f) fix(test): cargo test fails if redis is not running (#3352)
 * [27e5c31b5](https://github.com/numaproj/numaflow/commit/27e5c31b5fb02130055b05bd5a6c9b2fc4a33ba0) refactor(test): replace loop in #3350 with backoff::Retry (#3351)
 * [24d88d808](https://github.com/numaproj/numaflow/commit/24d88d8083015d526b755980f0c35d698bdb31c9) fix: replace single sleep before fetching pending count with a timeout (#3350)
 * [4a2aa2d00](https://github.com/numaproj/numaflow/commit/4a2aa2d0075a453550f48ed2e1f652f84ad2bfc9) chore(deps): bump github.com/go-jose/go-jose/v4 from 4.1.3 to 4.1.4 (#3348)
 * [89db25fb8](https://github.com/numaproj/numaflow/commit/89db25fb8258b1538c46df41e6d0782faf1de717) chore(deps): bump github.com/nats-io/nats-server/v2 from 2.11.12 to 2.11.15 (#3329)
 * [5f55c8130](https://github.com/numaproj/numaflow/commit/5f55c8130fd47bd97bb04b4623d8b58b354664eb) chore(deps): bump lodash from 4.17.23 to 4.18.1 in /ui (#3345)
 * [798c6534c](https://github.com/numaproj/numaflow/commit/798c6534cc56ef31dca718c92d464b6900d1e001) chore(deps): bump picomatch from 2.3.1 to 2.3.2 in /ui (#3333)
 * [aa2da6c94](https://github.com/numaproj/numaflow/commit/aa2da6c940fc302003c7a6f34f5e846738ea7730) chore(deps): bump @xmldom/xmldom from 0.8.10 to 0.8.12 in /ui (#3343)
 * [1af0d2e52](https://github.com/numaproj/numaflow/commit/1af0d2e52e885f9b4be57cd7e1cd84a26ffdb6d3) feat: http port support for http source (#3341)
 * [45e722ab8](https://github.com/numaproj/numaflow/commit/45e722ab8f9d8d4e346e3ec405040fef08007fd0) chore(deps): bump node-forge from 1.3.2 to 1.4.0 in /ui (#3339)
 * [45ecac783](https://github.com/numaproj/numaflow/commit/45ecac7833258b0ec46dde04215e79b51900387a) feat: Support configuring port for http source (#3335)
 * [5a2894295](https://github.com/numaproj/numaflow/commit/5a2894295fac4b88bd130a0dcefaad637d0a5b04) chore(deps): bump yaml from 2.4.5 to 2.8.3 in /ui (#3332)
 * [6cbe3a7aa](https://github.com/numaproj/numaflow/commit/6cbe3a7aa9bed38c2bf4d89970ba02eb9673419e) refactor: Merge Heartbeat and OT Buckets for Watermark Progression (#3287)
 * [d021d7215](https://github.com/numaproj/numaflow/commit/d021d72150602a1213f66ae6585c36951918f48c) doc: ordered processing end-to-end example (#3328)
 * [f820fc651](https://github.com/numaproj/numaflow/commit/f820fc6517e69b9e91f3873b63917c02e3b30857) fix: map stream can ack inflight messages during panic and tests (#3267)
 * [ccb932232](https://github.com/numaproj/numaflow/commit/ccb9322322d9c2a1c5470d48963245e74d2f774f) chore(deps): bump jsonpath from 1.2.1 to 1.3.0 in /ui (#3326)
 * [58afba469](https://github.com/numaproj/numaflow/commit/58afba469987923dfb737f822534f5f642f2910c) chore(deps): bump flatted from 3.3.1 to 3.4.2 in /ui (#3325)
 * [189dc451a](https://github.com/numaproj/numaflow/commit/189dc451a9f39e552e63dbbad6530260c9fec249) chore(deps): bump google.golang.org/grpc from 1.75.0 to 1.79.3 (#3323)
 * [38b0ff84d](https://github.com/numaproj/numaflow/commit/38b0ff84d0fb701336f2bee786438e903430bb4a) opex: emit SQS metrics (#3315)
 * [ab8e64cc4](https://github.com/numaproj/numaflow/commit/ab8e64cc4db2f126728f71523aff80ae24387267) refactor(style): remove unnecessary else blocks after return in util functions (#3314)
 * [fcfa118bd](https://github.com/numaproj/numaflow/commit/fcfa118bd79c9480ab33a0f5c542ef13271e7722) perf: precompile DNS1035 regex as package-level variable (#3311)
 * [c7e6a9b1d](https://github.com/numaproj/numaflow/commit/c7e6a9b1d93c59390cecc1f1f0ee0f44b8a3e6d7) fix: remove trailing comma in UniqueStringList.ToString (#3309)
 * [a2c8dad17](https://github.com/numaproj/numaflow/commit/a2c8dad17816be04e2a0f0166a10de5764e91241) fix: Utilize to vertex's ordered processing enabled config when building ToVertexConfig (#3306)
 * [d680b2ec5](https://github.com/numaproj/numaflow/commit/d680b2ec5cc43334a0f74e3ed01d2b0b9d1972f0) doc: improve ordered doc (#3304)
 * [81c82e45f](https://github.com/numaproj/numaflow/commit/81c82e45f593cc86ab2883483a0eff6f264c2fb7) chore(deps): bump quinn-proto from 0.11.12 to 0.11.14 in /rust (#3301)
 * [df914f8ba](https://github.com/numaproj/numaflow/commit/df914f8ba25624e9faca1650886695bde87b305a) doc: Ordered Processing (#3297)
 * [c7626ffa7](https://github.com/numaproj/numaflow/commit/c7626ffa747dade6d049c59d1c33110c75094497) fix(test): update tokio runtime flavor used in the spawned OS thread for component servers (#3290)
 * [b4a2ece9a](https://github.com/numaproj/numaflow/commit/b4a2ece9a992aba651c70f2253e6b81ef46b4218) fix: pending for http to fix autoscaling (#3292)
 * [bb8775a10](https://github.com/numaproj/numaflow/commit/bb8775a10d70301ffb1cf7fe2c9914c53f9b5627) fix: Mapper UDF error propagation to UI (#3288)
 * [7b57de326](https://github.com/numaproj/numaflow/commit/7b57de3264f6855ef88c1f9dddfbf75df6b38af4) fix: calc pending and watermark for ordered (#3286)
 * [3878a1251](https://github.com/numaproj/numaflow/commit/3878a1251cdc5310755b480d5376258af0d30cb2) fix: shuffle by key for partition-affinity if ordered processing is set (#3285)
 * [7d2a9ab64](https://github.com/numaproj/numaflow/commit/7d2a9ab6447bc6e356669503aed9a2e76fc43efd) feat(controller): fix scaling for ordered processing and validations (#3281)
 * [2061716cb](https://github.com/numaproj/numaflow/commit/2061716cbcdf38038f4dfb799af5b4794db8dadf) chore(deps): bump github.com/nats-io/nats-server/v2 from 2.10.27 to 2.11.12 (#3259)
 * [26fe61592](https://github.com/numaproj/numaflow/commit/26fe6159223949718a97af88105b7f540d9d4c8b) chore(deps): bump go.opentelemetry.io/otel/sdk from 1.38.0 to 1.40.0 (#3280)
 * [dc3f33d55](https://github.com/numaproj/numaflow/commit/dc3f33d55779e7386577acca86a0de90276cad96) chore(deps): bump rollup from 2.79.2 to 2.80.0 in /ui (#3279)
 * [e5412dc67](https://github.com/numaproj/numaflow/commit/e5412dc673e49502d735e2048c8e2529d8b5b8ed) feat(controller): enable ordered processing spec (#3278)
 * [c74dd1731](https://github.com/numaproj/numaflow/commit/c74dd1731ca66bbd6f00a04d317e178851c3b760) fix: check child resources health in isbsvc health check API (#3269)
 * [555a4d58b](https://github.com/numaproj/numaflow/commit/555a4d58b35e34f6c3c2dadc1f6f10382a032bd1) fix: Handle udsource disconnect when ACK is invoked (#3265)
 * [30db082d7](https://github.com/numaproj/numaflow/commit/30db082d796419014ecc46666fe4a5344a013c7f) refactor: nit fixes related to typos, doc comments, method names (#3266)
 * [797ae0c36](https://github.com/numaproj/numaflow/commit/797ae0c363862259ea94bce0425a7e7f6f316a76) feat: in-memory kv store with impl KVStore for testing (#3261)
 * [cc3a2f04d](https://github.com/numaproj/numaflow/commit/cc3a2f04d419802273a8059320fb9320d04e45e5) doc: data-plane streaming architecture (#3258)
 * [92cca2511](https://github.com/numaproj/numaflow/commit/92cca25118fdd18240ef1a2ae13b56bffdf7acf6) refactor: use KVStore trait for watermark operations (#3256)
 * [8af454b45](https://github.com/numaproj/numaflow/commit/8af454b4507324ca3f47e25ca81072d6158de4ae) refactor: trait for Key/Value Store with impl for Jetstream (#3253)
 * [d80075e2c](https://github.com/numaproj/numaflow/commit/d80075e2ca2fa9e63f9bf915ba32ab02934f0e8d) chore(doc): metrics doc updation (#3242)
 * [7fd5d103f](https://github.com/numaproj/numaflow/commit/7fd5d103f0ad7c35e5ed3fc2f983ed35adbbf55c) fix: minor fixes for reduce  (#3241)
 * [b4b7618c0](https://github.com/numaproj/numaflow/commit/b4b7618c00329e2aee0972f1d3ac5f5671666481) chore(perf): use trait_variant instead of async_trait (#3250)
 * [f3b347ef9](https://github.com/numaproj/numaflow/commit/f3b347ef96591d8562bc940a6ffe203da996f4fd) refactor: enable both h2, http/1.1 for rust mvtx daemon server (#3180)
 * [9ae3b5c54](https://github.com/numaproj/numaflow/commit/9ae3b5c5441be81f7c47e1dfe7216d617500d6d5) refactor: object-safety for ISBWriter and ISBReader  (#3245)
 * [f69400549](https://github.com/numaproj/numaflow/commit/f694005490ee2e2b1adc4befe2c3656ea9755436) chore(deps): bump jsonpath from 1.2.0 to 1.2.1 in /ui (#3230)
 * [9dad6d9b4](https://github.com/numaproj/numaflow/commit/9dad6d9b48036a1e29f12475b2f57a26a1503bcc) chore(ci): increase timeout for unit tests to 20 minutes (#3233)
 * [07630beb8](https://github.com/numaproj/numaflow/commit/07630beb862f1f8c7af477a0fa053a19093f50a6) fix(UI): side input fix (#3228)
 * [f19135de1](https://github.com/numaproj/numaflow/commit/f19135de1e15e81684c982d3cbf4b2dbea69ee8f) feat(ui): horizontal source+transformer row and sink arrow layout. (#3224)
 * [88746e369](https://github.com/numaproj/numaflow/commit/88746e36959a127d4485282b620c55d3439cc508) feat: check initContainerStatuses are in Waiting or Terminated state during health check  (#3226)
 * [a8c41c520](https://github.com/numaproj/numaflow/commit/a8c41c5208ded53ac7b11934e4d7d5f6f3080b3f) fix(UI): pending chart for source vertex (#3209)
 * [f602de7aa](https://github.com/numaproj/numaflow/commit/f602de7aa3948e7c329f1f2e121fb2f44acfe16f) feat: use initContainerStatuses in health checks (#3201)
 * [e971d9744](https://github.com/numaproj/numaflow/commit/e971d974404f21da12370de4ef9f4d4d4f14fe02) refactor: introduce ISBFactory for creating ISB reader/writer (#3217)
 * [74a27a051](https://github.com/numaproj/numaflow/commit/74a27a051f4f5789d460db6619098f8ba7f35a99) feat(test): a simple in-memory buffer for simulating ISB (#3213)
 * [dbdfdbd6d](https://github.com/numaproj/numaflow/commit/dbdfdbd6dce802503b6f3c74a13422b494f9552f) perf: Use Intra-task concurrency in source transformer (#3212)
 * [713f472e7](https://github.com/numaproj/numaflow/commit/713f472e78c95acb454c7558970a7e8758dcfb76) chore(deps): bump webpack from 5.94.0 to 5.105.0 in /ui (#3207)
 * [85af629b3](https://github.com/numaproj/numaflow/commit/85af629b313702042e1a6bc8e4b4687549c175d3) chore(deps): bump jsonpath from 1.1.1 to 1.2.0 in /ui (#3196)
 * [47c78c876](https://github.com/numaproj/numaflow/commit/47c78c8766e9ff96600db2a47eb3c58717747e3c) fix: use vertex name as partition label for source pending metric (#3205)
 * [a21b12d49](https://github.com/numaproj/numaflow/commit/a21b12d495a8d41814520e4dd3ba290ec94ebe3e) feat(ui): show onSuccess and fallback sink container icons in Pipeline graph (#3200)
 * [7f8ff2522](https://github.com/numaproj/numaflow/commit/7f8ff2522310bab139984b189b0e00c4ae6e4a5a) chore(deps): bump time from 0.3.41 to 0.3.47 in /rust (#3199)
 * [498175d70](https://github.com/numaproj/numaflow/commit/498175d70121603cad2643033c9ac40a9e9f6edf) chore(perf): Avoid message cloning in Sink (#3203)
 * [7f12e5ea3](https://github.com/numaproj/numaflow/commit/7f12e5ea3178da0e21435401522f5f85cd6839f8) fix: Restart on non-retryable kafka source ack errors (#3194)
 * [b189126a1](https://github.com/numaproj/numaflow/commit/b189126a1dfbbf7f1b5b240aefaf5bc02dbd10bd) fix(UI): force graph remount when pipeline structure changes (#3197)
 * [47e791dc7](https://github.com/numaproj/numaflow/commit/47e791dc7c2cc14d77356105b9e9b6f8421d8fa1) fix(UI): minor improvements for dark mode (#3195)
 * [72ca34456](https://github.com/numaproj/numaflow/commit/72ca34456091306c8e812f0195edfdee5baf516c) refactor: introducing tasks for map operations (#3167)
 * [4e8e72bb2](https://github.com/numaproj/numaflow/commit/4e8e72bb28bc1210f44e75003f69070f4ba1cbde) refactor: `write` and `resolve` should return the same type and Orchestrator to orchestrate retries (#3190)
 * [9adab30aa](https://github.com/numaproj/numaflow/commit/9adab30aa09dc1329d7b6256e8bf6f744a7dbe18) feat(ui): add dark mode theme (#3187)
 * [838c53447](https://github.com/numaproj/numaflow/commit/838c534474b9d57b25e396b6ff8f7daf4e9ac861) chore(deps): bump bytes from 1.10.1 to 1.11.1 in /rust (#3186)
 * [7ff51b94d](https://github.com/numaproj/numaflow/commit/7ff51b94da050ee252d5e9e697553a7caee63d1f) feat: ISB Writer Trait integration (propagated via NumaflowTypeConfig) (#3178)
 * [4e2447ab8](https://github.com/numaproj/numaflow/commit/4e2447ab807ec8c6aef9bd683014bf06182728b8) feat: add ISB Reader Trait and add it as Associated Type in NumaflowTypeConfig (#3175)
 * [9dedb64b5](https://github.com/numaproj/numaflow/commit/9dedb64b5ee2f2f2feb2f601a17439de448e0657) Add Starboard to USERS.md (#3169)
 * [58f8349e5](https://github.com/numaproj/numaflow/commit/58f8349e5bebbfa75363979c00d036f8c79f949b) doc(map): call out unary, batch, and streaming (#3164)
 * [39e34c56b](https://github.com/numaproj/numaflow/commit/39e34c56b493dab07a6171ba6aa58487faa04719) doc: Update FAQs with a scenario for `Server Info File not ready` (#3165)
 * [d84453065](https://github.com/numaproj/numaflow/commit/d844530650e0eb8626b23f59c75711099748334f) lint(clippy): fix all the remaining clippy issues (#3161)
 * [09c1db512](https://github.com/numaproj/numaflow/commit/09c1db512fd878ab68a632522d9772349b1c0206) lint(clippy): remaining(12/165) (#3159)
 * [a3983d64b](https://github.com/numaproj/numaflow/commit/a3983d64b7497c610e77619d8c600c00e1baa786) lint(clippy): remaining(105/165) (#3158)
 * [221e448a3](https://github.com/numaproj/numaflow/commit/221e448a39a100743e00c5dae26379c42d7f7955) lint(clippy): clippy fix 144/165 (#3157)
 * [b9d976f45](https://github.com/numaproj/numaflow/commit/b9d976f456ac5d67d7307c0b2efe9b17d97b6960) lint: fail if cargo check has warnings (#3156)
 * [0fd8e0e2d](https://github.com/numaproj/numaflow/commit/0fd8e0e2dfc41f4ab3c4695116f499986c7266f1) feat: SQS System Attributes and Custom Attributes Propagation (#3095)
 * [eb5d6a5e7](https://github.com/numaproj/numaflow/commit/eb5d6a5e748620bb71869f440695f29c71efb16d) refactor: error types and handling for Writer (#3151)
 * [b375dae2b](https://github.com/numaproj/numaflow/commit/b375dae2b6e3ac93ae03eadb4cc3521a92061068) chore(deps): bump lodash from 4.17.21 to 4.17.23 in /ui (#3149)
 * [d43565c53](https://github.com/numaproj/numaflow/commit/d43565c5349a687998c3468946bd2de83c215cf5) refactor: define errors for ISB Reader (#3148)
 * [26be9e502](https://github.com/numaproj/numaflow/commit/26be9e50260492847ec772d4906a85bba4faacd8) feat: Mvtx short-circuiting using a bypass router struct (#3126)
 * [fb5762d1e](https://github.com/numaproj/numaflow/commit/fb5762d1e3d8ebdc3a173bde738760586adaf6c2) chore(refactor): more idiomatic rust for server info (#3130)
 * [86d5a8dc0](https://github.com/numaproj/numaflow/commit/86d5a8dc0799dbe12a1f972891c5bb5a1546989c) feat (UI): support `onSuccess` sink in MonoVertex graph (#3110)
 * [1de6f1d11](https://github.com/numaproj/numaflow/commit/1de6f1d11889e51f5bd1b9dc8618e5728c1554cb) feat: Spec changes for short-circuiting mvtx (#3105)
 * [412c13efc](https://github.com/numaproj/numaflow/commit/412c13efc5060c628c5a2b03829043c927c88bce) doc: Documentation for on-success sink (#3107)
 * [0d69308c8](https://github.com/numaproj/numaflow/commit/0d69308c8800dfca0e2f406c84ec8d1332fe1db6) fix: Remove godebug env var initialization (#3092)
 * [e6220b1e9](https://github.com/numaproj/numaflow/commit/e6220b1e9d0acf78fc98ddc6d98dceb3957b1376) doc: add fallback container for  mvtx sink configuration (#3097)
 * [3754d5f8a](https://github.com/numaproj/numaflow/commit/3754d5f8a56e46c68060cd4f988f8073ae28baef) fix: graceful shutdown of sigkills in map udf container (#3096)
 * [5353cacd5](https://github.com/numaproj/numaflow/commit/5353cacd5da5a40444ec5d5316eabba0381192a7) fix: alignment and text wrapping in Errors tab (#3078)
 * [2b2114587](https://github.com/numaproj/numaflow/commit/2b2114587e167c35054af7527ac0dc82b894771f) chore(deps): bump node-forge from 1.3.1 to 1.3.2 in /ui (#3088)
 * [2c5aa9326](https://github.com/numaproj/numaflow/commit/2c5aa9326ffb4477cb89adaebd2a5697d0b4b108) feat: enable OTLP metrics exporter for daemon services (#3086)
 * [21f645a05](https://github.com/numaproj/numaflow/commit/21f645a05493b23f3eb0618914fc3b6095b6c236) doc: Update timeout doc for accumulator where idle detection config is required (#3085)
 * [e814963a1](https://github.com/numaproj/numaflow/commit/e814963a1841b6055876a9366d41cb4f2b5a9703) doc: update watermark doc with init source delay (#3084)
 * [4fe907330](https://github.com/numaproj/numaflow/commit/4fe907330ad851010ca21c22b0dd294ba44f1f2d) fix: use std context (#3081)
 * [75c61580b](https://github.com/numaproj/numaflow/commit/75c61580be66e7e9070a7251182c80bfe5761b89) doc: a bit more content to pipeline document (#3075)
 * [db4380b00](https://github.com/numaproj/numaflow/commit/db4380b00e044134d28bd380f40a1496995e6ee0) fix: add back drop metrics after sink refactor (#3074)
 * [ab53acfdb](https://github.com/numaproj/numaflow/commit/ab53acfdb5f3254463920cf96d00dcd13a6da9bf) fix: build issues in #3071 (#3072)
 * [ac8896476](https://github.com/numaproj/numaflow/commit/ac88964760bbb5d87b3c536d07380c5bfe034bfd) fix(test): we need 5 sec ticker in test (#3069)
 * [0cdf46704](https://github.com/numaproj/numaflow/commit/0cdf4670422c0d5960520e8c4282119b68c29705) fix: cron parser uses 6 field format (#3068)
 * [74885f5de](https://github.com/numaproj/numaflow/commit/74885f5de2d3049a05badd3ad3ac65d4cd90353b) doc: add map to mvtx example (#3066)
 * [bd4b057b3](https://github.com/numaproj/numaflow/commit/bd4b057b3dfa0090f341ca2b51d0e7c54b2c607d) feat: add support for OnSuccess sink (#3040)
 * [0745eba8f](https://github.com/numaproj/numaflow/commit/0745eba8fe00cd9ee3ec0b1e7723fda5ee6f4c88) fix: Kafka source/sink - pass client certs for mTLS when insecureSkipVerify is false (#3059)

### Contributors

 * Aayush Sapkota
 * Abdullah Yildirim
 * Adarsh Jain
 * Da.Sanchez
 * DattoDarragh
 * Derek Wang
 * Dillen Padhiar
 * Jagjot Bisram
 * Julie Vogelman
 * Junie Mariam Varghese
 * Keran Yang
 * Mikael Sundberg
 * Sreekanth
 * Sri Harsha Yayi
 * Surya Pratap Singh
 * Timothy
 * Vaibhav Kant Tiwari
 * Vaibhav Tiwari
 * Vigith Maurice
 * Yashash Lokesh
 * dependabot[bot]
 * shrivardhan

## v1.7.5 (2026-04-20)

 * [a11afd244](https://github.com/numaproj/numaflow/commit/a11afd24464cea8c57641b5daa97aee8e569dafd) Update manifests to v1.7.5
 * [94090b6dc](https://github.com/numaproj/numaflow/commit/94090b6dc29862f817fbc70911f489a3d4b834fd) chore(cherry-pick): downgrade health check error log to warn (#3108) (#3387)
 * [02cfc5743](https://github.com/numaproj/numaflow/commit/02cfc57437da6a88c78ce4280cc77002cd31cd58) Cherry pick commits for release 1.7.5 (#3386)

### Contributors

 * Vaibhav Kant Tiwari
 * Vaibhav Tiwari

## v1.7.4 (2026-03-13)

 * [be04ae805](https://github.com/numaproj/numaflow/commit/be04ae805ed4a9742e9be1711e6271fc12548c1f) Update manifests to v1.7.4

### Contributors

 * Yashash Lokesh

## v1.7.3 (2026-02-22)

 * [90a31118d](https://github.com/numaproj/numaflow/commit/90a31118d901a7ce067d5f33774a33b942fa325f) Update manifests to v1.7.3
 * [9252e08ad](https://github.com/numaproj/numaflow/commit/9252e08ad7d2d9e67036f9dfa6050f0cdceb0554) fix: sum of rate metrics across all partitions (#3243)

### Contributors

 * Yashash
 * Yashash Lokesh

## v1.7.2 (2026-02-13)

 * [f107d3b30](https://github.com/numaproj/numaflow/commit/f107d3b30e2a5f9161daf1942dbf3a8fdec903fd) Update manifests to v1.7.2
 * [a072ad0b7](https://github.com/numaproj/numaflow/commit/a072ad0b75cb50507c55111a989d8a540ca35197) fix(UI): pending chart for source vertex (#3209)
 * [7f2c98901](https://github.com/numaproj/numaflow/commit/7f2c989012b828e99a6e42fe94011a5bfde7ceb7) feat: check initContainerStatuses are in Waiting or Terminated state during health check  (#3226)
 * [1a679c72a](https://github.com/numaproj/numaflow/commit/1a679c72adafa010f0618262c30e21674a524942) fix: numa container hangs occassionally when map panics
 * [9f9b9c604](https://github.com/numaproj/numaflow/commit/9f9b9c6041a88645e6af8f82f73c78591e395661) feat: use initContainerStatuses in health checks (#3201)
 * [e8dfb2fe6](https://github.com/numaproj/numaflow/commit/e8dfb2fe602e01d7ff17f2a8b1360c7fbeba192f) feat: SQS System Attributes and Custom Attributes Propagation (#3095)
 * [9f875b2bd](https://github.com/numaproj/numaflow/commit/9f875b2bd69af1331c9a1a1d15aea4c242775b2d) fix: Restart on non-retryable kafka source ack errors (#3194)
 * [aec4f3a2e](https://github.com/numaproj/numaflow/commit/aec4f3a2ee51893da98fdd3b333c2f4c6af256a4) fix: use vertex name as partition label for source pending metric (#3205)

### Contributors

 * Adarsh Jain
 * Dillen Padhiar
 * Mikael Sundberg
 * Sreekanth
 * Yashash
 * shrivardhan

## v1.7.1 (2026-01-17)

 * [7c8b4926c](https://github.com/numaproj/numaflow/commit/7c8b4926cbd39066136c7d95c9862e21ddaa5095) Update manifests to v1.7.1
 * [b72b8f4f6](https://github.com/numaproj/numaflow/commit/b72b8f4f63b2f546c33efa969bbace8fb60546b0) fix: use std context (#3081)
 * [f713f246e](https://github.com/numaproj/numaflow/commit/f713f246e479a55184afa82b2025492197dab8af) fix: graceful shutdown of sigkills in map udf container (#3096)
 * [e7b62d2e7](https://github.com/numaproj/numaflow/commit/e7b62d2e7ac5788df2e2dbf6b7ce57b0cf7247f7) fix: alignment and text wrapping in Errors tab (#3078)
 * [b1e599689](https://github.com/numaproj/numaflow/commit/b1e599689b6edc86994e39bec7198cd15169070a) fix: add back drop metrics after sink refactor (#3074)

### Contributors

 * Adarsh Jain
 * Vigith Maurice
 * Yashash Lokesh

## v1.7.0 (2025-11-11)

 * [2d68ff453](https://github.com/numaproj/numaflow/commit/2d68ff4536274a659b7282a2a65cac210f64e523) Update manifests to v1.7.0
 * [e6a9f3c8d](https://github.com/numaproj/numaflow/commit/e6a9f3c8d027b716886dcf4269ebb95c46da7470) fix: build issues in #3071 (#3072)

### Contributors

 * Sreekanth
 * Yashash H L

## v1.7.0-rc1 (2025-10-30)

 * [0bbf43b9d](https://github.com/numaproj/numaflow/commit/0bbf43b9d621a16a1cf822de27ee8371208ad2cc) Update manifests to v1.7.0-rc1
 * [362f942a0](https://github.com/numaproj/numaflow/commit/362f942a006c25ed110b56573242c37cd683b28a) fix: remove from tracker after windows are created and fix concurrency issues with rater (#3051)
 * [367d2121b](https://github.com/numaproj/numaflow/commit/367d2121bbf6e97e874e835ab691db33e55d8c17) refactor: watermark to not use actor pattern (#3039)
 * [3c727dc57](https://github.com/numaproj/numaflow/commit/3c727dc57c1f5ea154ff868bf0223d19ed3480a2) fix: watermark values that are displayed in UI (#3032)
 * [caa292288](https://github.com/numaproj/numaflow/commit/caa292288e7869ebc5b8290cf5680a4c5981199e) doc: add SDK examples for Accumulate function (#3036)
 * [d16d948e2](https://github.com/numaproj/numaflow/commit/d16d948e2f85045ec4d5c92930bc08ec8c597b2d) doc: content tabs for side input in python/Go/Java (#3033)
 * [a33e45bdd](https://github.com/numaproj/numaflow/commit/a33e45bdddb089f1b562d1b5c68558158a5e6348) refactor: Sink Component (#3021)
 * [543b4708b](https://github.com/numaproj/numaflow/commit/543b4708bde6a9f5a4f4557d312139a227ed8f84) fix: active partitions for source watermark publisher (#3022)
 * [b5ed6c9d8](https://github.com/numaproj/numaflow/commit/b5ed6c9d83cd040a29b771c3e4d195b2f0572122) fix: [::]:50051 is not parsable by ASP.NET.CORE 9  (#3020)
 * [e9769ec26](https://github.com/numaproj/numaflow/commit/e9769ec26a45c7bb3eaba4cdec1264c018de4c43) fix: Progress watermark for a source idling from the start (#3015)
 * [20d2d816f](https://github.com/numaproj/numaflow/commit/20d2d816fa6c848d213eb4a0fb2aef8bcf154435) fix(doc): point docs to the new python path (#3018)
 * [4ffad22c8](https://github.com/numaproj/numaflow/commit/4ffad22c893308feda227e56abfba0313ddc538e) fix: delete the correct nats Secret if it exists (#3014)
 * [c20e716ee](https://github.com/numaproj/numaflow/commit/c20e716eeece3b344c882465c2668619b83868f5) refactor: ISB Reader and Writer (#2986)
 * [4fc2af4b1](https://github.com/numaproj/numaflow/commit/4fc2af4b1f852041239f2c7d58e87326275f6530) feat(UI): add fallback sink container in MonoVertex graph  (#3004)
 * [46c9cfe9d](https://github.com/numaproj/numaflow/commit/46c9cfe9d3ce8589152d4262edafe170a2d580a4) feat(UI): add udf(map) container in Monovertex graph (#3000)
 * [73f4dd24a](https://github.com/numaproj/numaflow/commit/73f4dd24ac69fbc16b2ec76e626af958574c22c1) feat: enable map in monovertex (#2994)
 * [f438691bf](https://github.com/numaproj/numaflow/commit/f438691bf4c9738e4a5aaa2249023b547676eacf) fix: autoscaling for low throughput non-source vertices (#2996)
 * [0f1210163](https://github.com/numaproj/numaflow/commit/0f1210163df50cb9a93b39fdecad1665bf0332b5) docs(autoscaling): why a built-in autoscaler is needed (#2993)
 * [05d7ae1d0](https://github.com/numaproj/numaflow/commit/05d7ae1d05f68fd7b13c13db0e3f33ce5254919a) doc: Documentation for resumedRampUp (#2991)
 * [6b5288187](https://github.com/numaproj/numaflow/commit/6b5288187a5c66f890b55c0aa83d40956f095eb9) feat: Implement immediate ramp up during re-deployments (#2985)
 * [94039dc0f](https://github.com/numaproj/numaflow/commit/94039dc0f4b2562e436e71a0a69148be80b83e43) fix: make Tags mandatory in ForwardConditions to match Rust backend (#2970)
 * [9c31c7837](https://github.com/numaproj/numaflow/commit/9c31c78378f8e2dafdae584fe034452175416ec6) fix(ci): replace bitnami/kafka with apache/kafka with kRaft (#2988)
 * [3f8989ffb](https://github.com/numaproj/numaflow/commit/3f8989ffb60163c3794a4b7a8d92e356438d8cd9) doc: Document throttling modes (#2987)
 * [e25b2628d](https://github.com/numaproj/numaflow/commit/e25b2628de93b50e705cb2dbd5ac12b8c927d7d9) feat: Control plane wiring for only_if_used and go_back_n modes.  (#2973)
 * [6595a63bf](https://github.com/numaproj/numaflow/commit/6595a63bf901dfb6e41912701ef87248f001cc4a) fix: duplicate envs for monitor container (#2983)
 * [bf399a52e](https://github.com/numaproj/numaflow/commit/bf399a52e12741dfb816ba1452cdc13c021af774) fix(UI): watermark fetch apis for reduce edges (#2965)
 * [33f112b91](https://github.com/numaproj/numaflow/commit/33f112b91d362a67f4560bb1790284985d690958) fix:  udf write metrics in streaming mode (#2974)
 * [1b3d65ea8](https://github.com/numaproj/numaflow/commit/1b3d65ea8aa310cd908d4fb5f186f45494f1b151) feat: Deposit token logic and add OnlyIfUsed and GoBackN mode (#2962)
 * [12e28a5b0](https://github.com/numaproj/numaflow/commit/12e28a5b0868aaae0a74738da2964d56725d44b4) feat: add assume role for SQS (#2963)
 * [6a4a4c938](https://github.com/numaproj/numaflow/commit/6a4a4c9386f08281e710dc6bb6f1ebaa6b9a1623) feat: adding nack support for sources (#2952)
 * [2d8b12dc0](https://github.com/numaproj/numaflow/commit/2d8b12dc0c69551ecf2a38c121278d7401e35a76) fix: return none, if any of the Vn-1 processors are not idle (#2948)
 * [ea3d65d15](https://github.com/numaproj/numaflow/commit/ea3d65d15a9e9478f2d6e373b6adecd073f6f8ea) fix: rater should not consider nil metric data points (#2958)
 * [bc428e461](https://github.com/numaproj/numaflow/commit/bc428e4610b84e686f101d6665d781248bc68fff) feat: Wiring for scheduled rate limiting mode (#2954)
 * [1eb8016ce](https://github.com/numaproj/numaflow/commit/1eb8016cea86ff143c5890474db4b0d18142d0d1) fix: sqs sink batch entry ID (#2951)
 * [0ca814e65](https://github.com/numaproj/numaflow/commit/0ca814e65836544095caa5d1b22682bcc45a96ae) feat: introduce throttling modes (#2949)
 * [5e5062693](https://github.com/numaproj/numaflow/commit/5e506269349b981bc8ff0a08a097519a0c0fe8ee) fix: fallback sink validation (#2947)
 * [1d022dabc](https://github.com/numaproj/numaflow/commit/1d022dabcdb8e8ab0bf296ab1f1550874e63e02e) fix: flaky rater test by removing truncation  (#2945)
 * [f575d53b9](https://github.com/numaproj/numaflow/commit/f575d53b9ce5f6a2d7fa4aa01d112825bc87ba1f) fix: Rater should use ticker tick's timestamp for window (#2935)
 * [252105a51](https://github.com/numaproj/numaflow/commit/252105a51b799451b8960dee621b8735c7b46600) fix: reader should determine the idling (#2932)
 * [1e8ffc996](https://github.com/numaproj/numaflow/commit/1e8ffc996a944413aae84daebf2f14708535840b) fix: `side_input_operations` test should be behind `nats-tests` feature (#2933)
 * [391490565](https://github.com/numaproj/numaflow/commit/39149056542da6929146f4f10be88d99c3567c5f) fix: Forward Kafka message keys (#2931)
 * [7d4b652ce](https://github.com/numaproj/numaflow/commit/7d4b652ce6cc40d8bcb6b1d55d2c8e8666dfdcc9) fix: detect terminating pods as soon as possible (#2919)
 * [04c36cfcc](https://github.com/numaproj/numaflow/commit/04c36cfcc84492d5e75b8eb49aaf6e57e4267674) fix: timeout increased for a flaky ui test and check rust formatting in lint target (#2927)
 * [e6b139524](https://github.com/numaproj/numaflow/commit/e6b139524c2cb5e18d5a3f5227ca29fbabf312a2) feat: Redis Sentinel Mode Support For Rate Limiter (#2917)
 * [87ea7d554](https://github.com/numaproj/numaflow/commit/87ea7d554f6b472ddbf81b9d3216f819d3470390) chore(deps): bump tracing-subscriber from 0.3.19 to 0.3.20 in /rust (#2920)
 * [b84b86072](https://github.com/numaproj/numaflow/commit/b84b860729a3c4f6d1b8d45309eca05cd1241dab) fix: Add `spec.replicas` field to minimal CRDs (#2910)
 * [a3f4a5ea4](https://github.com/numaproj/numaflow/commit/a3f4a5ea4364fb4b2de6fcd75fcbcd67894aad93) feat: Integration of Distributed Throttler With Numaflow Data-plane (#2904)
 * [48cfbe989](https://github.com/numaproj/numaflow/commit/48cfbe989d5d3d2c35210015f538c8c081dcb50b) fix: create service for http source in mvtx (#2908)
 * [6c7d3eff5](https://github.com/numaproj/numaflow/commit/6c7d3eff586b531963bb67408538f353cc6335fd) fix: ack messages which should not be forwarded to any vertices (#2905)
 * [0e867e4ce](https://github.com/numaproj/numaflow/commit/0e867e4cecc338f91871168ea77f6960bcb9e601) fix(UI): loading state when vertex has 0 pods (#2862)
 * [630d6dd02](https://github.com/numaproj/numaflow/commit/630d6dd02a8ab166866d5e19c5d0b134ee04a2c6) feat: timeout support for rate limiter (#2896)
 * [3ade3f919](https://github.com/numaproj/numaflow/commit/3ade3f9190c1f75a620952e0daafb17cc7cc3a23) feat: framework for distributed throttling (#2892)
 * [1b1895524](https://github.com/numaproj/numaflow/commit/1b189552486a2e15fa2694348d1f4892227bed3e) doc: document compression (#2885)
 * [eaa2b21a6](https://github.com/numaproj/numaflow/commit/eaa2b21a681e95f120d86f318718e18d9a94200f) chore(deps): bump slab from 0.4.10 to 0.4.11 in /rust (#2880)
 * [06f4a04b2](https://github.com/numaproj/numaflow/commit/06f4a04b2edab997cd3007a75c807e5f428b7bd8) doc: update compatibility (#2868)
 * [5ea47661f](https://github.com/numaproj/numaflow/commit/5ea47661fe36a15093ffee46ed84306d78c0c0b6) fix: default read timeout for pipeline config (#2878)
 * [d09c31c4b](https://github.com/numaproj/numaflow/commit/d09c31c4bfc0d2cff43b4343ca478f3eb0e1dc2e) fix: flaky `timeAlgo` UI test (#2870)
 * [665553a55](https://github.com/numaproj/numaflow/commit/665553a55a6d6106339ebab2c3a15967c98b9983) Enhance gRPC error logging to include metadata and details (#2864)
 * [12c4d4e09](https://github.com/numaproj/numaflow/commit/12c4d4e096c868afa1c18851036965eea2a72be2) Use ReadWriteOncePod as default PVC access mode (fixes #2854) (#2865)
 * [ee27ded47](https://github.com/numaproj/numaflow/commit/ee27ded47760474417b34f86d84ecb4166e42c0b) fix: decode error details of gRPC status (#2866)
 * [424970f94](https://github.com/numaproj/numaflow/commit/424970f94c89aef10610f27b75cc301b5edb122a) doc: add python accumulator url (#2859)
 * [a8bb86b9a](https://github.com/numaproj/numaflow/commit/a8bb86b9a80383f3673537467e71e86ca3e62a66) Golang made Numaflow possible, Rust for infinity and beyond (#2857)

### Contributors

 * Aayush Sapkota
 * Adarsh Jain
 * Da.Sanchez
 * Derek Wang
 * Julie Vogelman
 * Martin Thøgersen
 * Rachel McGuigan
 * Roman Alexander
 * Siddhant Khare
 * Sreekanth
 * Vaibhav Kant Tiwari
 * Vigith Maurice
 * Yashash
 * Yashash H L
 * dependabot[bot]
 * shrivardhan
 * yogesh

## v1.6.0 (2025-08-02)

 * [e19334ed7](https://github.com/numaproj/numaflow/commit/e19334ed78513f7e557ae545659ba247eea78ddd) Update manifests to v1.6.0
 * [2fa2c81fa](https://github.com/numaproj/numaflow/commit/2fa2c81faee1636908e1e24d1947eab28530c479) make lint
 * [a3129bb30](https://github.com/numaproj/numaflow/commit/a3129bb30ad7b98e4ab4d5e7dce5fc1a5891ad4e) fix: revert golang to 1.23.1
 * [f2d45fbc2](https://github.com/numaproj/numaflow/commit/f2d45fbc2cc84b36f0260484c5f870d61b719057) fix: requeue reconciliation after seeing container restart (#2855)
 * [d3643ceb6](https://github.com/numaproj/numaflow/commit/d3643ceb6da68f2ef9a4f8929c6d1c90475750a8) fix: graceful shutdown during reduce udf panic (#2845)
 * [839b42d04](https://github.com/numaproj/numaflow/commit/839b42d04c96b6581db9e6f4b39353fab882d3a2) feat: unpause pipelines vertices and mvtx from min replicas (#2840)
 * [3d295bd5e](https://github.com/numaproj/numaflow/commit/3d295bd5ecb24da966100fc84a1fee302c5f3d84) feat: adaptive lookback and pending metrics at daemon for pipeline (#2834)
 * [2d33bb869](https://github.com/numaproj/numaflow/commit/2d33bb86987bef7d11dd639e51767d71c1d62a9f) fix: infinite render for pipeline/mvtx card (#2844)
 * [11039b322](https://github.com/numaproj/numaflow/commit/11039b322c3a4fb11a757590822f1c9b792f0b3a) feat: enable rust runtime by default (#2833)
 * [b15986d0c](https://github.com/numaproj/numaflow/commit/b15986d0cb5964324303e6d9013ab944599d521f) feat: Filter subjects and deliver policy as user options (#2837)
 * [fb90083d4](https://github.com/numaproj/numaflow/commit/fb90083d48f61271e4a0472cb86733457b791e8b) fix: add serving pipeline transformer config (#2835)
 * [50f0833fe](https://github.com/numaproj/numaflow/commit/50f0833fecf640e1d45b61942eb73c85cf133d5d) fix: handle stream already existing when making consumer (#2832)
 * [c9e44386e](https://github.com/numaproj/numaflow/commit/c9e44386e5fa25f07d9ec7a7a330802fd7d6a03b) fix: Detect tokio worker threads (#2831)
 * [101e6db7e](https://github.com/numaproj/numaflow/commit/101e6db7e907b9b448bde012a536b729a7b43c57) chore(deps): bump form-data from 3.0.1 to 3.0.4 in /ui (#2829)
 * [6d56a6d58](https://github.com/numaproj/numaflow/commit/6d56a6d58d7114e39a70e0d4cbaebaa3a9102cce) fix: aligned and unaligned reduce in Rust runtime (#2827)
 * [afdd16f7a](https://github.com/numaproj/numaflow/commit/afdd16f7adc9217430a505c779a36832f6273497) feat: nats source in rust (#2804)
 * [e64424f97](https://github.com/numaproj/numaflow/commit/e64424f97f635081cd42d9d424924872435d6599) doc: User documentation for Jetstream source (#2826)
 * [279e4ee5f](https://github.com/numaproj/numaflow/commit/279e4ee5fc23e97e59fcb84a9ca3ca74cb308be5) fix: Jetstream source - Option to specify consumer name (#2805)
 * [abb70d4cf](https://github.com/numaproj/numaflow/commit/abb70d4cf7fd55c5d6a963d11b8d298f0a22fee5) fix: accumulator last seen time (#2825)
 * [a8d15e49e](https://github.com/numaproj/numaflow/commit/a8d15e49ec3ed9ad2f40fd7f6bc9a84506c883f1) chore(deps): bump golang.org/x/oauth2 from 0.25.0 to 0.27.0 (#2822)
 * [7dec45fd6](https://github.com/numaproj/numaflow/commit/7dec45fd6520e1c75e09cc579af4281bb69048be) feat(core): extended proto format for metadata (#2821)
 * [100fa1cf1](https://github.com/numaproj/numaflow/commit/100fa1cf19f3e6a1918ab3dd0d6c61588c5cd28d) feat: jetstream isb metrics in rust (#2813)
 * [2824b210d](https://github.com/numaproj/numaflow/commit/2824b210d194cacae75672e3054e916c28734fbd) fix: flaky fetch head watermark unit test (#2808)
 * [2bcbabc78](https://github.com/numaproj/numaflow/commit/2bcbabc7805c6013174951aeccabd4ec20c5d6bc) fix(UI): health filter & support for mono-vertex health on pipeline listing page (#2761)
 * [0d972a669](https://github.com/numaproj/numaflow/commit/0d972a6691a1c64361f822fb47ffb4fee9078c54) fix: `TestDropOnFull` e2e test (#2803)
 * [0f847d831](https://github.com/numaproj/numaflow/commit/0f847d831a6126c474921672241223118d1050b8) feat: Daemon server to use http based watermark fetcher (#2798)
 * [8fefa40f7](https://github.com/numaproj/numaflow/commit/8fefa40f7efc57bc9b902d9f01157b1b87a39301) fix: kafka writes messages to same partition when setKey is not set (#2797)
 * [013d03d59](https://github.com/numaproj/numaflow/commit/013d03d59435f6851b3cfe6079521f0a22152b34) chore(test): Improve testing for side-input (#2801)
 * [258f9f475](https://github.com/numaproj/numaflow/commit/258f9f475bd8de75eea64ead2d3482ed6acc54a4) feat: e2e tests with rust runtime (#2693)
 * [8b4ee04a0](https://github.com/numaproj/numaflow/commit/8b4ee04a0e60393828441c541dcb4b8c2f376e30) fix: drain input channel during batch map shutdown (#2793)
 * [a3fe0db5f](https://github.com/numaproj/numaflow/commit/a3fe0db5ff382fc9fd8a2a6d6c243fc8ea15f08f) chore(doc): roadmap 1.6 (#2792)
 * [f73a4e4cf](https://github.com/numaproj/numaflow/commit/f73a4e4cf79800afa0ac2445184e6ddb3e711064) fix: failures in e2e tests ignored (#2791)
 * [79156da4a](https://github.com/numaproj/numaflow/commit/79156da4a085c08e8e53f1caa132225813634229) fix: cli opts and clap (#2788)
 * [59b023aa5](https://github.com/numaproj/numaflow/commit/59b023aa55c98bf8df69ac5496b2d56b937a0bb6) fix: exit code conditional rendering (#2789)
 * [082573039](https://github.com/numaproj/numaflow/commit/082573039ec45a397dc2eb5659c935548079d7eb) fix: add `-mv-` in mvtx name for pod memory/cpu metrics (#2784)
 * [8c6065984](https://github.com/numaproj/numaflow/commit/8c60659848cf93ce004d5bee37dbf0cf30f23931) doc: Example PDB and Anti-Affinity config for ISB (#2787)
 * [efd0ee9cb](https://github.com/numaproj/numaflow/commit/efd0ee9cb6ada379352ddd46ea5c5e3e40e13e92) fix: jetstream source should use published timestamp (#2778)
 * [10e5aec1a](https://github.com/numaproj/numaflow/commit/10e5aec1aad9d84c43d651d990c68570e504466b) fix: use kafka timestamp as event time (#2781)
 * [766a6a5a5](https://github.com/numaproj/numaflow/commit/766a6a5a57e9d8ae817b29688cc312059ea16935) fix: allow subcommands other than monitor and serving in cli (#2776)
 * [76e2b27bd](https://github.com/numaproj/numaflow/commit/76e2b27bdb977149dac52c5f177adf09d93371ae) Fix: Jetstream source - Unlimited reconnects (#2772)
 * [33bd2b93d](https://github.com/numaproj/numaflow/commit/33bd2b93d4c641a9e7211bec752b20f89a4290b3) fix: vertex type to avoid panics in watermark manager (#2766)
 * [d84980e20](https://github.com/numaproj/numaflow/commit/d84980e200c90c8dababba535752f0cec85fd408) fix: Generator source - Base64 decode user specified blob (#2757)
 * [000272599](https://github.com/numaproj/numaflow/commit/0002725990ea71e26d95705bf95d4249609b0620) fix: potential deadlock in async data movement window manager implementation (#2755)
 * [b404420be](https://github.com/numaproj/numaflow/commit/b404420bef1d5c6eb9835d7ec7d61547203472ce) feat: Unaligned Window for Async Data Movement (#2699)
 * [6a62d8e7a](https://github.com/numaproj/numaflow/commit/6a62d8e7a25eb23b6567ab39fd5b1213ce40ddde) feat: HTTP Basic auth support for Pulsar source/sink (#2749)
 * [514ff0edc](https://github.com/numaproj/numaflow/commit/514ff0edc116ddb023b5516024566b609a02564d) fix(UI): pending and ackPending buffer info on Edges (#2746)
 * [29eb00f70](https://github.com/numaproj/numaflow/commit/29eb00f70100bd34679a5e6351a7d3cf69eb5957) test(ui): partially fix UI test (#2726)
 * [79733d9b1](https://github.com/numaproj/numaflow/commit/79733d9b16c880ae58ef127f5be2743474a91038) feat: Pulsar Sink implementation (#2745)
 * [71a3274e6](https://github.com/numaproj/numaflow/commit/71a3274e60539c95f268cb0157a77b4d33684b6d) feat: insert id into generator payload (#2735)
 * [dcd5a1412](https://github.com/numaproj/numaflow/commit/dcd5a1412733114ab3810ea6539653643665750c) feat: http source should honor response status codes sent to clients (#2732)
 * [296db5e76](https://github.com/numaproj/numaflow/commit/296db5e766d51f3b5ae81c823a27846daa40f414) feat(ci): enable lychee link checking for examples directory (#2729)
 * [fabe1313c](https://github.com/numaproj/numaflow/commit/fabe1313c0d18fe489136ef0396db281786d4e4f) fix: wait for inflight messages to be processed in transformer (#2731)
 * [85579b633](https://github.com/numaproj/numaflow/commit/85579b63349f5c945795fba50c9d795206e83d6a) fix: revisit some metrics in rust (#2692)
 * [13aff8f2a](https://github.com/numaproj/numaflow/commit/13aff8f2ac2c319f3c10531418da6d69540b898b) feat: add serving http server (#2633)
 * [c44e89ead](https://github.com/numaproj/numaflow/commit/c44e89eadafbaf9e001d1cfdb2d6f3f2ce133e21) fix: Wrong usage of get_consumer_from_stream (#2724)
 * [1b1a0dac0](https://github.com/numaproj/numaflow/commit/1b1a0dac016650808ef88f04926fe17f0193379b) fix: map udf metrics (#2588)
 * [c026d4a12](https://github.com/numaproj/numaflow/commit/c026d4a12043150a4f836f59a84322e31d98b5ed) feat: Kafka Rust implementation - Oauth authentication (#2718)
 * [3c7fc15f9](https://github.com/numaproj/numaflow/commit/3c7fc15f940144d13bbe9ae68d6dd7e7c089a5c8) fix: mount runtime path for pipeline/vertex (#2716)
 * [fd897c8dc](https://github.com/numaproj/numaflow/commit/fd897c8dc9faf7db821580d3058d8fdc70c2c01b) chore(ci): lychee only for md files (#2713)
 * [f6a42f255](https://github.com/numaproj/numaflow/commit/f6a42f2551f323b3dd7c0a8c7946ebf3d019719c) chore(ci): with more tests being added it is timing out (#2710)
 * [3d4e7e773](https://github.com/numaproj/numaflow/commit/3d4e7e7733e2cfb244d701cf8a3fdab2462567d0) fix: source watermark publisher in async data movement (#2707)
 * [811275f60](https://github.com/numaproj/numaflow/commit/811275f60012427a9c6196cad8bc0ab176664b6d) feat: optionally enable Wire compression to optimize network and ISB stability (#2709)
 * [768ac9477](https://github.com/numaproj/numaflow/commit/768ac9477e6531e145c01e609bf122dbe2cdbdfa) fix: flaky unit test (#2702)
 * [f4334baca](https://github.com/numaproj/numaflow/commit/f4334bacad5580f94c4babce427bfe7af84775a8) fix: Rust HTTP source - More validations on HTTP headers (#2700)
 * [2cab60c2a](https://github.com/numaproj/numaflow/commit/2cab60c2a1ddde0f0272b6570144071a49c4e94b) fix: http port (#2697)
 * [6d673fd3d](https://github.com/numaproj/numaflow/commit/6d673fd3dc2769b5f3d10172fb74cb99bcbd960c) feat: Allow specifying JVM Kafka opts directly (#2689)
 * [d14f685ff](https://github.com/numaproj/numaflow/commit/d14f685ffd46e9772bb73c03fe731f623c0f9e7f) feat: http integration (#2691)
 * [6bf5247a5](https://github.com/numaproj/numaflow/commit/6bf5247a5c4c1189540c4860bdec5c732d075b63) fix: reduce UDF metrics (#2624)
 * [ea48171e1](https://github.com/numaproj/numaflow/commit/ea48171e193d1d4749b191c53284564989cb96c4) fix: typo in transformer config (#2688)
 * [e6506094d](https://github.com/numaproj/numaflow/commit/e6506094d20e9a51e442c34556525d6ecb1bbaa9) feat: extension for http source (#2687)
 * [a1afdf15f](https://github.com/numaproj/numaflow/commit/a1afdf15fc64c01fe3c68a15957346d0f5c3f24a) feat: Kafka sink implementation in Rust (#2672)
 * [d4193580a](https://github.com/numaproj/numaflow/commit/d4193580a223cb4b0505af9378b3cbebedb25124) fix: retry attempts calculation and logs in retry strategy (#2653)
 * [073001182](https://github.com/numaproj/numaflow/commit/073001182180d91d20a94a40b836b85023a54b83) fix: return 400 if serving ID header contains '.' (#2669)
 * [53a87ee7b](https://github.com/numaproj/numaflow/commit/53a87ee7b2ab30ba60c7ec7b9293e6a32e3fc867) fix: conditional forwarding in Async Dataplane (#2668)
 * [546561ab4](https://github.com/numaproj/numaflow/commit/546561ab40f59d2e79d0ab2e9fee2402d594d9b1) feat: Rust pipeline metrics (#2666)
 * [278a750da](https://github.com/numaproj/numaflow/commit/278a750da996311444e4efe5eb498c307d0ceead) feat: Aligned Window for Reduce Async Data Movement  (#2618)
 * [af3038961](https://github.com/numaproj/numaflow/commit/af3038961049b355a7c2a3793570abc93b1c3b83) feat: Kafka source - Rust implementation (#2636)
 * [9c2e36ce4](https://github.com/numaproj/numaflow/commit/9c2e36ce436c4a604441739b1acceac469cc1006) fix: potential incorrect k8s minor version (#2647)
 * [54277d00e](https://github.com/numaproj/numaflow/commit/54277d00ea25c22ebacfd6415d94ab371fac2fe6) feat: exponential backoff retry strategy for sink (#2614)
 * [730c9ebc6](https://github.com/numaproj/numaflow/commit/730c9ebc62b0306adbf2d9f7d2fc9a3e11eca0eb) fix: honour retry config in case of Pipeline in Rust (#2638)
 * [396107205](https://github.com/numaproj/numaflow/commit/3961072057ccfc6b6b7c88f06ef7846aa5d48d6d) fix: broken retry strategy in sink (#2626)
 * [2edbd0b08](https://github.com/numaproj/numaflow/commit/2edbd0b082508f46ba326e364d584f5c1904af35) fix: sink instantiation (#2627)
 * [caab77bfe](https://github.com/numaproj/numaflow/commit/caab77bfee2fc41822c59ee80cf4c8b74af151dd) feat: sqs sink (#2615)
 * [9b341effb](https://github.com/numaproj/numaflow/commit/9b341effb6cd9bdb30e762e06250e826af03ac17) fix: source and transformer metrics (#2584)
 * [5c2875ef2](https://github.com/numaproj/numaflow/commit/5c2875ef2a79971ac9abf0b78e8f7f285c4affc3) feat: enable pod template support for serving deployment (#2620)
 * [d5f22ec73](https://github.com/numaproj/numaflow/commit/d5f22ec73423bbf80baf32ced2d292e9607a6c93) fix: serving example (#2619)
 * [76ecdc63b](https://github.com/numaproj/numaflow/commit/76ecdc63b439e24b9acd025dfba5d47c47fed25b) fix: take only the base filename while sorting (#2616)
 * [09742d25d](https://github.com/numaproj/numaflow/commit/09742d25d18423c38cb2eecb70cb3297f3c73874) fix: idle watermark severity (#2613)
 * [de8a98d64](https://github.com/numaproj/numaflow/commit/de8a98d6444b99cc47099c201572433c2ca43d91) doc: minor typo in accum doc (#2611)
 * [d2a76cc07](https://github.com/numaproj/numaflow/commit/d2a76cc07386814981e496420c10ef0adcbff906) doc: accumulator doc (#2609)
 * [2c055c0e3](https://github.com/numaproj/numaflow/commit/2c055c0e38a155fb811f3bb53c04e09e24ce69ed) feat: PBQ for Reduce Async Data Movement (#2601)

### Contributors

 * Adarsh Jain
 * Derek Wang
 * Kyle Cooke
 * Sidhant Kohli
 * Sreekanth
 * Takashi Menjo
 * Vedant Gupta
 * Vigith Maurice
 * Yashash H L
 * dependabot[bot]
 * shrivardhan
 * yogesh

## v1.5.3 (2025-09-23)

 * [01f244f99](https://github.com/numaproj/numaflow/commit/01f244f99327cf788d46b1632c74be125198e937) Update manifests to v1.5.3
 * [80f4282d4](https://github.com/numaproj/numaflow/commit/80f4282d4a58a9fe73cfd14ecea21d46d27fa29f) fix: sqs invalid unit tests
 * [e69d2bbbc](https://github.com/numaproj/numaflow/commit/e69d2bbbcb6bea9dfaefc2b2306cf1be2e7170aa) feat: add assume role for SQS (#2963)
 * [15cfc474b](https://github.com/numaproj/numaflow/commit/15cfc474b78121bc1ac9102e596caf94f89ec04f) fix: sqs sink batch entry ID (#2951)
 * [d9e768d43](https://github.com/numaproj/numaflow/commit/d9e768d435639e7dbccf013c72f9ccd321486f92) fix: fallback sink validation (#2947)
 * [37df8e039](https://github.com/numaproj/numaflow/commit/37df8e0395bbc15403aeeff220df0703df4cc53a) fix: Add `spec.replicas` field to minimal CRDs (#2910)
 * [33a58c2d6](https://github.com/numaproj/numaflow/commit/33a58c2d65ebb57e982774b0996a1e769c429597) fix: requeue reconciliation after seeing container restart (#2855)

### Contributors

 * Derek Wang
 * Julie Vogelman
 * Yashash H L
 * shrivardhan

## v1.5.2 (2025-08-01)

 * [27e8e0cac](https://github.com/numaproj/numaflow/commit/27e8e0cac1a4820110d7e9e2d886bf27e66c3ccb) Update manifests to v1.5.2
 * [b5962c783](https://github.com/numaproj/numaflow/commit/b5962c7832373da5bf34e05cb514295e736e37f5) feat: unpause pipelines vertices and mvtx from min replicas (#2840)

### Contributors

 * Derek Wang
 * Sidhant Kohli

## v1.5.1 (2025-07-09)

 * [4f70d50ea](https://github.com/numaproj/numaflow/commit/4f70d50eab6b066c831025e5d494f3c5b6e3f69c) Update manifests to v1.5.1
 * [b43ad8406](https://github.com/numaproj/numaflow/commit/b43ad8406e4f0b13d96de7ea45c091ba789425de) fix: drain input channel during batch map shutdown (#2793)
 * [018a4e608](https://github.com/numaproj/numaflow/commit/018a4e608172615e45ca42efe9a262710c7526d2) fix: publish larger watermark when offset is same
 * [e17b4d0c9](https://github.com/numaproj/numaflow/commit/e17b4d0c9d74dfdc012c4174fc3958c9c29a7d00) fix: exit code conditional rendering (#2789)
 * [79f9a7568](https://github.com/numaproj/numaflow/commit/79f9a7568ada48f74b67f7d51d2b4ca191649ead) fix: add `-mv-` in mvtx name for pod memory/cpu metrics (#2784)
 * [bce5f53a1](https://github.com/numaproj/numaflow/commit/bce5f53a15675a9f577e7c628fcaa07136ce5fb9) fix: use kafka timestamp as event time (#2781)
 * [3108bf947](https://github.com/numaproj/numaflow/commit/3108bf94705b0375ca98848adfff1560efd2262a) Fix: Jetstream source - Unlimited reconnects (#2772)
 * [737b70417](https://github.com/numaproj/numaflow/commit/737b704172ac5b03c7556ba50dd5de5bb92c469e) fix: Generator source - Base64 decode user specified blob (#2757)
 * [94329c326](https://github.com/numaproj/numaflow/commit/94329c32636b1f198437749a4e0faac608fb9d3c) fix: revert golang to 1.23.1
 * [0eda460c3](https://github.com/numaproj/numaflow/commit/0eda460c3e7ca6a274832c620ecafacb5c81232a) fix: jetstream source should use published timestamp (#2777)

### Contributors

 * Adarsh Jain
 * Sreekanth
 * Vedant Gupta
 * Vigith Maurice
 * Yashash H L

## v1.5.0 (2025-06-18)

 * [96790b95a](https://github.com/numaproj/numaflow/commit/96790b95a604da675133b4432dc14240ead7ec9a) Update manifests to v1.5.0
 * [7dc6b95b5](https://github.com/numaproj/numaflow/commit/7dc6b95b50e5bfcfb1bdb3bae1b96f0e8290b1c6) feat: insert id into generator payload (#2735)
 * [cbcfe7a1e](https://github.com/numaproj/numaflow/commit/cbcfe7a1e7d3e50396cc61743b0763f17eefc25c) feat: http source should honor response status codes sent to clients (#2732)
 * [8c104a56c](https://github.com/numaproj/numaflow/commit/8c104a56c9e655a616fa407973466109b12dbfd0) feat(ci): enable lychee link checking for examples directory (#2729)
 * [06c011cb2](https://github.com/numaproj/numaflow/commit/06c011cb22072dcbe3d81ae9a75ba23d840ca9f2) fix: wait for inflight messages to be processed in transformer (#2731)
 * [2abd9d3d1](https://github.com/numaproj/numaflow/commit/2abd9d3d193647e2bbdf61efa094aeba84acc1ae) fix: revisit some metrics in rust (#2692)
 * [7b634dd95](https://github.com/numaproj/numaflow/commit/7b634dd955202afb7525fa1d2c328e202328d7f4) feat: add serving http server (#2633)
 * [b85f6d33f](https://github.com/numaproj/numaflow/commit/b85f6d33fbac2a3bbf827b0b918ac7bfa8351559) fix: Wrong usage of get_consumer_from_stream (#2724)
 * [5dbf2fac8](https://github.com/numaproj/numaflow/commit/5dbf2fac8be1a44358cac347cbbef7f0b02f863d) fix: map udf metrics (#2588)
 * [74335a291](https://github.com/numaproj/numaflow/commit/74335a291f09b46bcc3bb118c0236d2bf39cb5b8) feat: Kafka Rust implementation - Oauth authentication (#2718)
 * [565293ce1](https://github.com/numaproj/numaflow/commit/565293ce1401f16a63acfafac2bd159971559ab9) fix: mount runtime path for pipeline/vertex (#2716)
 * [0f8a461e1](https://github.com/numaproj/numaflow/commit/0f8a461e14c73a4fb5a1a3d5868d2e4618cdb0a5) chore(ci): lychee only for md files (#2713)
 * [3961e47e8](https://github.com/numaproj/numaflow/commit/3961e47e88e1176e68987596f57f4142f2b349ee) chore(ci): with more tests being added it is timing out (#2710)
 * [ca9ad6943](https://github.com/numaproj/numaflow/commit/ca9ad694333920e7c18857ff07fa5a4aab6b9f49) fix: source watermark publisher in async data movement (#2707)
 * [8c2b564f3](https://github.com/numaproj/numaflow/commit/8c2b564f3a1f69d60d4058bce4aa4a819fdc85e7) feat: optionally enable Wire compression to optimize network and ISB stability (#2709)
 * [435ea8271](https://github.com/numaproj/numaflow/commit/435ea82718604be3084ea1c4cfcd46d3d0a48a63) fix: flaky unit test (#2702)
 * [aee84310a](https://github.com/numaproj/numaflow/commit/aee84310a65d8bd98defe7243f2d2d216830f3d5) fix: Rust HTTP source - More validations on HTTP headers (#2700)
 * [652d45474](https://github.com/numaproj/numaflow/commit/652d454740b46ca57befc78593dc8af68ef2f3ff) fix: http port (#2697)
 * [c3e45031c](https://github.com/numaproj/numaflow/commit/c3e45031cf8ff7c2532b539135cfa73fadb907c5) feat: Allow specifying JVM Kafka opts directly (#2689)
 * [10c041c86](https://github.com/numaproj/numaflow/commit/10c041c86d8de3793abdde1c5ad52b0b7d230af1) feat: http integration (#2691)
 * [6b84334e5](https://github.com/numaproj/numaflow/commit/6b84334e52a66bead10663a6be27fd187aa71f49) fix: reduce UDF metrics (#2624)
 * [34a2c9a81](https://github.com/numaproj/numaflow/commit/34a2c9a81721c8639da549cb05c2751f1837795b) fix: typo in transformer config (#2688)
 * [11a5b6cde](https://github.com/numaproj/numaflow/commit/11a5b6cde0f3d49aebdbca1e3b390cc0fa751882) feat: extension for http source (#2687)
 * [046d1f7d3](https://github.com/numaproj/numaflow/commit/046d1f7d3b25903b17f49f0697470a29d457fab3) feat: Kafka sink implementation in Rust (#2672)
 * [770a7a29a](https://github.com/numaproj/numaflow/commit/770a7a29af821a2a7472705474e4ce6c33e74ac4) fix: retry attempts calculation and logs in retry strategy (#2653)

### Contributors

 * Adarsh Jain
 * Derek Wang
 * Sreekanth
 * Takashi Menjo
 * Vigith Maurice
 * Yashash H L
 * shrivardhan
 * yogesh

## v1.5.0-rc5 (2025-05-24)

 * [7a8d496a3](https://github.com/numaproj/numaflow/commit/7a8d496a3acd375180ffe2023e373e418528441e) Update manifests to v1.5.0-rc5
 * [4389a6f6f](https://github.com/numaproj/numaflow/commit/4389a6f6fed4a31bec5802d2411a1e08ab95ac45) fix: return 400 if serving ID header contains '.' (#2669)
 * [690ea0dd6](https://github.com/numaproj/numaflow/commit/690ea0dd68da068b20fc53febccf6c62c67b387a) fix: conditional forwarding in Async Dataplane (#2668)
 * [954cc5bb0](https://github.com/numaproj/numaflow/commit/954cc5bb037dd354d698fdd1c59f4eb9385e5ce5) feat: Rust pipeline metrics (#2666)
 * [e053a5cec](https://github.com/numaproj/numaflow/commit/e053a5cec6004cc9afee928fa5f752a0d889a1d5) feat: Aligned Window for Reduce Async Data Movement  (#2618)
 * [5a3d32014](https://github.com/numaproj/numaflow/commit/5a3d320147ed84bcd30251cd15de6692e298d736) feat: Kafka source - Rust implementation (#2636)

### Contributors

 * Adarsh Jain
 * Derek Wang
 * Sreekanth
 * Vigith Maurice
 * Yashash H L

## v1.5.0-rc4 (2025-05-20)

 * [9f52ffc6a](https://github.com/numaproj/numaflow/commit/9f52ffc6a4bba38f077fcaeb04b4e3080006935c) Update manifests to v1.5.0-rc4

### Contributors

 * Derek Wang

## v1.5.0-rc3 (2025-05-18)

 * [408391237](https://github.com/numaproj/numaflow/commit/408391237923536f482099a1040f02b104832375) Update manifests to v1.5.0-rc3
 * [abc72d3bb](https://github.com/numaproj/numaflow/commit/abc72d3bb4e1f70d990ee07dcc5dc51030b5d768) fix: potential incorrect k8s minor version (#2647)
 * [f39686946](https://github.com/numaproj/numaflow/commit/f396869467d3b78ced4dfcc23267d082155911d4) feat: exponential backoff retry strategy for sink (#2614)
 * [b3a691a41](https://github.com/numaproj/numaflow/commit/b3a691a414241e6a74016f0a7cebd224b924c078) fix: honour retry config in case of Pipeline in Rust (#2638)
 * [35c80b14f](https://github.com/numaproj/numaflow/commit/35c80b14f2edc74dfb1cc078393776703e1231de) fix: broken retry strategy in sink (#2626)

### Contributors

 * Adarsh Jain
 * Derek Wang
 * Yashash H L

## v1.5.0-rc2 (2025-05-12)

 * [bec31d7fd](https://github.com/numaproj/numaflow/commit/bec31d7fd7fd444b3db33f7a97ca71fe19b96675) Update manifests to v1.5.0-rc2
 * [89e1d4f72](https://github.com/numaproj/numaflow/commit/89e1d4f724168864541bf09c9493088b03f88d68) fix: sink instantiation (#2627)
 * [ee7137347](https://github.com/numaproj/numaflow/commit/ee7137347ed091179bdc64bef4523e9d85a81b40) feat: sqs sink (#2615)
 * [8ede89a7c](https://github.com/numaproj/numaflow/commit/8ede89a7c4ea73fc7878148a839c6f42df6eaf31) fix: source and transformer metrics (#2584)
 * [10f7bd2b0](https://github.com/numaproj/numaflow/commit/10f7bd2b0be9aa5fff596c3599bf2f3ca8354623) feat: enable pod template support for serving deployment (#2620)
 * [532c236d4](https://github.com/numaproj/numaflow/commit/532c236d42878228dafeaa2c0f268e7067738005) fix: serving example (#2619)
 * [9430f277b](https://github.com/numaproj/numaflow/commit/9430f277bf40b26b1af82cf69d67c252e5850013) fix: take only the base filename while sorting (#2616)
 * [4bd05a7f0](https://github.com/numaproj/numaflow/commit/4bd05a7f01be010961af2408c343fbc696905b0f) fix: idle watermark severity (#2613)
 * [868addb02](https://github.com/numaproj/numaflow/commit/868addb02c7da8b40a5d7b81aa89a3aedec37db2) doc: minor typo in accum doc (#2611)
 * [af29f1590](https://github.com/numaproj/numaflow/commit/af29f15900a91d9ecca05abb8ed731ae0432aa13) doc: accumulator doc (#2609)
 * [224c8a4e6](https://github.com/numaproj/numaflow/commit/224c8a4e6cdae5e2cd2195d2f6d82bca5c8fcaf7) feat: PBQ for Reduce Async Data Movement (#2601)

### Contributors

 * Derek Wang
 * Shrivardhan Rao
 * Takashi Menjo
 * Vigith Maurice
 * shrivardhan

## v1.5.0-rc1 (2025-04-30)

 * [834446ff7](https://github.com/numaproj/numaflow/commit/834446ff701f1dd9ef2cdc347179e0544ce1c14a) Update manifests to v1.5.0-rc1
 * [4a6f72e70](https://github.com/numaproj/numaflow/commit/4a6f72e70a435b7d728632a8400432564e5a85f4) fix: minor optimizations in serving (#2585)
 * [3e54bb5e2](https://github.com/numaproj/numaflow/commit/3e54bb5e26dd8a7aef28a1be546f85b8dfb8cf6c) feat: integrate sqs source (#2403)
 * [8c6b7c868](https://github.com/numaproj/numaflow/commit/8c6b7c86859d8456bf6c059ee8e298fb8cd04490) feat: Improves Serving Latency and Throughput (#2564)
 * [4f79b7ecd](https://github.com/numaproj/numaflow/commit/4f79b7ecd41eb7fc669658515aa79edeec16cc56) fix(manifests): some manifests related to servingpipeline missing (#2580)
 * [071d215c7](https://github.com/numaproj/numaflow/commit/071d215c7e535e8cf46d982551d6ec73309de11f) feat: generic Write Ahead Log framework (#2571)
 * [163ba83a2](https://github.com/numaproj/numaflow/commit/163ba83a2429e6c3cb46e8303f77e9c126ed5410) fix: have `make pre-push -B` pass (#2567)
 * [51130f8c0](https://github.com/numaproj/numaflow/commit/51130f8c0ba39819fe6b6245b81594fd443b0133) fix: isb_redis_read_error_total's help message (#2568)
 * [c5a67019b](https://github.com/numaproj/numaflow/commit/c5a67019b05931a94459bc9910563089128b8d8a) feat: add missing metrics for mvtx (#2542)
 * [9c0b14d8c](https://github.com/numaproj/numaflow/commit/9c0b14d8c2bb483de4ec0f7d2f416fe24c7a5e17) feat: expose metrics for pipeline/mvtx desired and current phase (#2563)
 * [50141cd3c](https://github.com/numaproj/numaflow/commit/50141cd3c9dacb04972a99b7a2fbe4a3ce6ce6f2) fix: start persisting errors after first active pods update (#2545)
 * [cb6e6e65a](https://github.com/numaproj/numaflow/commit/cb6e6e65aab4b97739344056d3088bf9e073d7cb) feat: Support for pending count with Adaptive lookback  (#2486)
 * [1be8ff481](https://github.com/numaproj/numaflow/commit/1be8ff48186daa7c20efbdb9a8deb67ea80633ab) feat(Serving): option to specify container template (#2554)
 * [869771c5b](https://github.com/numaproj/numaflow/commit/869771c5bdf65745aa66f7e2d26e1c5c0a0de8a4) chore(deps): bump http-proxy-middleware from 2.0.7 to 2.0.9 in /ui (#2558)
 * [a9e529dda](https://github.com/numaproj/numaflow/commit/a9e529ddaa6b2c7fca4c9701de25f149ad66ca9e) chore(deps): bump golang.org/x/net from 0.36.0 to 0.38.0 (#2557)
 * [dd15ef66b](https://github.com/numaproj/numaflow/commit/dd15ef66b56c9307688eae36c0e2bb0f7d08216b) doc: add missing docs (#2552)
 * [eb4dd4399](https://github.com/numaproj/numaflow/commit/eb4dd43995f96739f725d86fc80c7d890b21f19e) doc: User docs for ServingPipeline (#2549)
 * [4242f68d6](https://github.com/numaproj/numaflow/commit/4242f68d660aebd521d24c5487f90d8d06b51349) chore(deps): bump github.com/nats-io/nats-server/v2 from 2.10.20 to 2.10.27 (#2555)
 * [2dfb86699](https://github.com/numaproj/numaflow/commit/2dfb86699675b111fa4cc98967055f05ff9f9b58) fix: format stacktrace (#2548)
 * [0e5603f14](https://github.com/numaproj/numaflow/commit/0e5603f14136e8f5c1bd09170c23bcade8f0a7f7) feat: Builtin serve sink (#2551)
 * [b4cfad32d](https://github.com/numaproj/numaflow/commit/b4cfad32d4ca9f52e4750715ba2728c7cb6f1ef4) feat: enhancing metrics tab and charts (#2544)
 * [d4948a1ae](https://github.com/numaproj/numaflow/commit/d4948a1aead74220d6bda790a83d7659580ce124) feat: persist error in case of partial/no responses from udf (#2539)
 * [bf7e1a289](https://github.com/numaproj/numaflow/commit/bf7e1a2896690ff06c62dda6eaafb26dcc9e4288) fix: sidecar health check for long running queries in async data movement (#2541)
 * [3706185fb](https://github.com/numaproj/numaflow/commit/3706185fb1b2c7b392905ab72af30eae831bc45a) feat: [experimental] start golang vs. rust image based on env (#2537) (#2538)
 * [99621c47e](https://github.com/numaproj/numaflow/commit/99621c47ee6306d9d3f85c1644fc6586a45f99e6) feat: Serving - Expose metrics (#2533)
 * [8b1b2eb2b](https://github.com/numaproj/numaflow/commit/8b1b2eb2b9bed66badc73cfdfb780849dbb518b0) chore(deps): bump tokio from 1.44.1 to 1.44.2 in /rust (#2531)
 * [91e66702b](https://github.com/numaproj/numaflow/commit/91e66702b7a6851ba9586bde279f71faabc3c886) feat: scaffold ServingPipeline (#2468)
 * [803869e39](https://github.com/numaproj/numaflow/commit/803869e3964f7c0b521189e81395a2451501e1d1) feat: errors tab for application errors (#2473)
 * [fff09d097](https://github.com/numaproj/numaflow/commit/fff09d0972d9551b6a814aeecf8f3819be9764fc) feat: vertex error endpoints (#2519)
 * [d3e2b69cf](https://github.com/numaproj/numaflow/commit/d3e2b69cf15138049def1704cd899a73eb3ec8c4) fix: create a new bidirectional stream for every partition to avoid race condition (#2521)
 * [66b1168f6](https://github.com/numaproj/numaflow/commit/66b1168f6b04028a9365dcdb6f6008b1f366fe7c) fix: add active pod function for runtime errors (#2515)
 * [64a73791b](https://github.com/numaproj/numaflow/commit/64a73791b34dc0f25994a7700338195084ab557a) feat: Jetstream source implementation in Rust (#2484)
 * [c55c879b1](https://github.com/numaproj/numaflow/commit/c55c879b109fd92dc32406fe304e3af0f511354f) chore(deps): bump openssl from 0.10.68 to 0.10.71 in /rust (#2505)
 * [62b993c6f](https://github.com/numaproj/numaflow/commit/62b993c6fc529bb7633662485cfd7be985d25175) feat: surface application errors (#2499)
 * [e6b262969](https://github.com/numaproj/numaflow/commit/e6b262969dcdb20398942bf17f892d72d2d7a26c) feat: validating webhook for MonoVertex (#2497)
 * [70993d568](https://github.com/numaproj/numaflow/commit/70993d56825c1dae02da2a2748233d1585666b5d) fix: typo, use timeout instead of ttl (#2494)
 * [dd96c6b59](https://github.com/numaproj/numaflow/commit/dd96c6b5910bda6906ab3f0c3c5dee5e64fe06c7) chore(deps): bump github.com/golang-jwt/jwt/v5 from 5.2.1 to 5.2.2 (#2493)
 * [b74201aca](https://github.com/numaproj/numaflow/commit/b74201aca6b5e95264a1ff66f6aa3ba19d176769) fix: main is broken (#2490)
 * [edd068ade](https://github.com/numaproj/numaflow/commit/edd068ade56a9bbf8a526dd53989bc3928f2725a) feat: Accumulator (#2475)
 * [e878bdf00](https://github.com/numaproj/numaflow/commit/e878bdf00c962d2188375b4c81c832116ced6f46) fix: concurrent invocations to update active pods list in rater (#2478)
 * [7027fdc22](https://github.com/numaproj/numaflow/commit/7027fdc22b36d331176ad11ad776833ea8b1fba3) fix(docs): PodMonitor manifest in metrics (#2477)
 * [6f3dc8105](https://github.com/numaproj/numaflow/commit/6f3dc8105ff3c906cff46bd36f78e8226ff242da) fix: Recreate KV watcher if it never returns any events (#2474)
 * [a54169039](https://github.com/numaproj/numaflow/commit/a54169039401a8a2b9b3a67fd26e0683c6b7d03f) doc(typo): explicit callout for map (#2476)
 * [751f34288](https://github.com/numaproj/numaflow/commit/751f34288c24ee77fa15482c9a90d867040dfd23) fix(docs): labels of ISB errors in metrics (#2463) (#2466)
 * [f55462303](https://github.com/numaproj/numaflow/commit/f554623036074e216142e39d4a75fa0d00ee476d) chore(deps): bump @babel/runtime from 7.24.8 to 7.26.10 in /ui (#2465)
 * [9ecbacb60](https://github.com/numaproj/numaflow/commit/9ecbacb60daf2d26361b89b83e22a24d8168b4db) chore(deps): bump golang.org/x/net from 0.34.0 to 0.36.0 (#2464)
 * [95ee94891](https://github.com/numaproj/numaflow/commit/95ee9489119b9da0bfead2f15a17d024dd8cb657) fix: legends overlapping metrics charts (#2453)
 * [51783837e](https://github.com/numaproj/numaflow/commit/51783837e70f4cc33e7b97686899e3be23af4a9e) feat: serving feature complete (#2422)
 * [50217e74e](https://github.com/numaproj/numaflow/commit/50217e74ef2664b28a061db5f2a175c71e5ebc45) chore(deps): bump ring from 0.17.8 to 0.17.13 in /rust (#2448)
 * [9a223c0ec](https://github.com/numaproj/numaflow/commit/9a223c0ecb692cdffab508e866841bdc4d57044d) fix(ci): upgrade deprecated runner (#2437)
 * [2869e0f80](https://github.com/numaproj/numaflow/commit/2869e0f809e91c687e7557fce7789337e2d35f9c) feat: attach emptyDir volume for runtime info (#2431)
 * [894674675](https://github.com/numaproj/numaflow/commit/894674675834428c1148ac2ff190094337a74c89) chore(deps): bump github.com/go-jose/go-jose/v4 from 4.0.1 to 4.0.5 (#2428)
 * [b97885b37](https://github.com/numaproj/numaflow/commit/b97885b3757c7a35a49fb71b10cc16a9f527fb5c) refactor: introducing lifecycle to Vertex spec (#2419)
 * [1dc2b31a1](https://github.com/numaproj/numaflow/commit/1dc2b31a1d871f68ca2bfb3cb4fe0d81e86d9730) doc(batchmap): clarify relation to readBatchSize (#2418)
 * [ef0cffdc6](https://github.com/numaproj/numaflow/commit/ef0cffdc67504ba7a97bbb779c97fdcfd4233056) fix(controller): replicas calculation for paused pipeline (#2412)
 * [ae6c2a6e1](https://github.com/numaproj/numaflow/commit/ae6c2a6e1c89c0a72a3e52fbce2af9fa5c029b9d) fix: error handling and grace shutdown during udf failures (#2406)
 * [01cc33d5c](https://github.com/numaproj/numaflow/commit/01cc33d5cc1cb1621cbbe21742a68a3c2cdf0d3b) feat: Serving as a builtin source with Monovertex (#2382)
 * [d5d44084a](https://github.com/numaproj/numaflow/commit/d5d44084ae98b89a77f982139cf3bfca6db03e31) fix: sorting metrics chart tooltip values (#2401)
 * [b9188460e](https://github.com/numaproj/numaflow/commit/b9188460e073f3fd8fd3ad7a98c783d7e0eeabcb) fix: filter out deamon pod and unnecessary containers from metrics output (#2394)
 * [6c397baec](https://github.com/numaproj/numaflow/commit/6c397baecdc1cbaffe0db1cb276d7abddc0509db) fix: error state & search/negate search in case of prev terminated logs (#2391)
 * [7d2251dd5](https://github.com/numaproj/numaflow/commit/7d2251dd5368f4eacec790f7c28cc3281283af8b) fix(controller): multiple issues related to min/max of a vertex (#2398)
 * [8d51c66af](https://github.com/numaproj/numaflow/commit/8d51c66af69eed9785cb98ed1c973afe66d6a6fe) feat: Idle Watermark Implementation Inside Async Data Movement for Source and ISB (#2385)
 * [bf0f9dba0](https://github.com/numaproj/numaflow/commit/bf0f9dba0577e13c4085f811a96ddf2ba59400ab) feat: add sqs source (#2355)
 * [72a11e9b3](https://github.com/numaproj/numaflow/commit/72a11e9b349d27b745bf0e08c61bc6b5cb888dd7) feat: updating chart lables and tooltip (#2387)
 * [52614f858](https://github.com/numaproj/numaflow/commit/52614f858ba54b2e79bfa2e3fa519c9bb53c9a52) feat: contextual flow for remaining metrics (#2379)
 * [12c272036](https://github.com/numaproj/numaflow/commit/12c272036f85b0ab979fe0b5e68e09443ce49806) feat: cpu memory metrics (#2332)
 * [493457110](https://github.com/numaproj/numaflow/commit/493457110176cdb098a19b1abb214cd5a084d7f1) feat: Controller changes to support Serving as a builtin source (#2357)
 * [208527d85](https://github.com/numaproj/numaflow/commit/208527d85490cdfc551c6560ae5e10a50c85f1c4) feat: moving ud containers logs as default in pods view (#2378)
 * [afc16ac0b](https://github.com/numaproj/numaflow/commit/afc16ac0bc2633071f2843b5eecf2de1b44159d0) feat: adaptive lookback for monovertex (#2373)
 * [8e9bafb7e](https://github.com/numaproj/numaflow/commit/8e9bafb7ee0e45eaf6f1e099e353965b53bf31d5) fix: timestamp and level filter fix for logs (#2374)
 * [084be8933](https://github.com/numaproj/numaflow/commit/084be8933d6d6eb6a6dea726671e0fef94472ee9) feat: Watermark Implementation for Async Data Movement (#2376)
 * [d4e5859a2](https://github.com/numaproj/numaflow/commit/d4e5859a21a8d39eec4dea7e145dde4b10e4b6fb) fix: get status of init containers in pods-info API call (#2371)
 * [5d6043195](https://github.com/numaproj/numaflow/commit/5d6043195bb85ef2ae2ea758c15fd084fac73f7b) fix: error handling in case of udsink failures (#2372)
 * [c621d2bcf](https://github.com/numaproj/numaflow/commit/c621d2bcf80a35536c03c20d968f103a7e11081a) fix(test): use different approach to start jetstream service (#2359)
 * [8cd95a8c1](https://github.com/numaproj/numaflow/commit/8cd95a8c1d9d58a631708970e0a67f118f2d52bf) feat: rows/page dropbox. Fixes #1764 (#2337)
 * [bdab75910](https://github.com/numaproj/numaflow/commit/bdab75910c28d4b200c4d5da6caa51fe5b71d5c7) fix: units and tooltip content for metric charts (#2333)
 * [c4925203e](https://github.com/numaproj/numaflow/commit/c4925203ea6d2bcdb9c5deeba5f1a11329ff122f) feat: implement callbacks from vertices when serving is used as source (#2311)
 * [3b563062b](https://github.com/numaproj/numaflow/commit/3b563062b7b3248afadbd5ddd20bffd597ac562e) fix: more criteria to detect lifecycle changes, fixes: #2331 (#2346)
 * [2515a4b3a](https://github.com/numaproj/numaflow/commit/2515a4b3acc3ef7d4b1fc5832c58060435a9df37) feat: contextual flow for metrics (#2345)
 * [f4feae724](https://github.com/numaproj/numaflow/commit/f4feae72467b0a573dd869a7e65217b6a624c8e6) chore(deps): bump golang.org/x/net from 0.29.0 to 0.34.0 (#2339)
 * [370767394](https://github.com/numaproj/numaflow/commit/3707673947f99e16a3b3849b079d629a1a1c1472) fix: clean up metrics for deleted mvtx (#2338)
 * [33b992b85](https://github.com/numaproj/numaflow/commit/33b992b852d14ad1788d8d0fc0af7b7d117d023e) feat: Enhance Serving To Support Flatmap Operations (#2324)
 * [376a6023b](https://github.com/numaproj/numaflow/commit/376a6023ba735e1d483862cf1a77b86009c59a1d) feat: logs enhancements (#2320)
 * [fd4a0aaca](https://github.com/numaproj/numaflow/commit/fd4a0aaca263c82a92340ac00f28bd77feba9f27) feat: expose pending messages metric in async pipeline (#2330)
 * [2653fb248](https://github.com/numaproj/numaflow/commit/2653fb248b19cbce4becdd348580a430a8950a20) feat: Added support for kafka OAuth authentication (#2257)
 * [ade70d8da](https://github.com/numaproj/numaflow/commit/ade70d8da310729129708781119a5634c65f9df0) fix: lastScaledAt not updated during autoscaling down (#2323)
 * [b6c4de183](https://github.com/numaproj/numaflow/commit/b6c4de1832f1f6b4c42338b119dc2470383177f5) fix: Initialize rustls's CryptoProvider early in the code (#2312)
 * [5f5af1bad](https://github.com/numaproj/numaflow/commit/5f5af1baddce04d1e93b5ef6e1976ac1828d1a85) feat: make unit and display name configurable (#2269)
 * [8d8340c72](https://github.com/numaproj/numaflow/commit/8d8340c725e39031ef56e04e603c3b9241f15399) feat: Implement `Sourcer` traits for serving source (#2301)
 * [d387d3fe5](https://github.com/numaproj/numaflow/commit/d387d3fe5bbcc9c5a2f181cebfa2c0ef3b15eb87) feat: enhance time range selector for metrics - include presets (#2292)
 * [812847652](https://github.com/numaproj/numaflow/commit/8128476528bf49ae742ff9259c5291bd0a6473e3) feat: Asynchronous Map Implementation for Pipeline (#2295)
 * [fc14696aa](https://github.com/numaproj/numaflow/commit/fc14696aa1d8c79c98e6d20e12bdc438d8c6a740) fix: set max decode size of proto message, add mvtx metrics (#2283)
 * [c05c96ac3](https://github.com/numaproj/numaflow/commit/c05c96ac3cfcc201121bc740d6908d03bd302831) chore(deps): bump golang.org/x/crypto from 0.27.0 to 0.31.0 (#2290)
 * [d94461541](https://github.com/numaproj/numaflow/commit/d94461541c41d2f0d7c68ebd2e73030e8e5a0095) chore(deps): bump nanoid from 3.3.7 to 3.3.8 in /ui (#2289)
 * [d75998fdd](https://github.com/numaproj/numaflow/commit/d75998fdd3fcb5a3ecc0ccfc3e47a8bc7f97c33b) feat: pipeline gauge metrics (#2284)
 * [059a585b0](https://github.com/numaproj/numaflow/commit/059a585b04c85415cea9f343a0cf08927100ba35) feat: counter metrics visualizer for mono-vertex (#2256)
 * [8bed236dc](https://github.com/numaproj/numaflow/commit/8bed236dc3f9f423d5f36bf5d259d0486c73bc76) feat: mvtx gauge metrics support (#2259)
 * [1c0989a96](https://github.com/numaproj/numaflow/commit/1c0989a9667b3c664bc965655c40bc05d4537384) fix: Numaflow serving sink (#2103)
 * [df9be77be](https://github.com/numaproj/numaflow/commit/df9be77bea7f769685241296a696bce78d446cfc) fix: "Loading Pods View" and "failed to get pod details" state on UI (#2248)
 * [7c07e0541](https://github.com/numaproj/numaflow/commit/7c07e0541ef3060ae8995c5359eb0e465c49b474) feat: Conditional Forwarding In Asynchronous Pipeline  (#2270)
 * [5f69064ed](https://github.com/numaproj/numaflow/commit/5f69064ed836d91e73947d43853830d89895e5ba) feat: Introducing Tracker to track completeness of the Message (#2264)
 * [11c0d2b40](https://github.com/numaproj/numaflow/commit/11c0d2b40c314d81517fe8afeea30edd50f3cda4) fix: Sink Config to respect Fallback (#2265)
 * [5c9094b32](https://github.com/numaproj/numaflow/commit/5c9094b3200737027bbcfdc0bced3528b7b8b405) fix: honor lookbackSeconds in monovertex and rust pipeline (#2258)
 * [05df88691](https://github.com/numaproj/numaflow/commit/05df88691d8c9a787c5d4b84e5a97bf4d9d39a4e) feat: counter metrics visualizer for pipeline (#2238)
 * [2530bade0](https://github.com/numaproj/numaflow/commit/2530bade00d6c2088a9e9d649e2de84ba22036a8) feat: Asynchronous Streaming for Source and Sink in Numaflow Core (#2251)
 * [b31380b14](https://github.com/numaproj/numaflow/commit/b31380b1445669fa91888fb2e3feec382773590e) feat: Built-in Pulsar source (#2237)
 * [e4bb2f726](https://github.com/numaproj/numaflow/commit/e4bb2f726a633fde7b12c6ef9b6874de5af1e572) fix: init container logs (#2245)
 * [f5a79bf9e](https://github.com/numaproj/numaflow/commit/f5a79bf9e808a32d4895a04ccdd048c377fd4549) feat: unify metrics ( cleanup and add missing metrics ) (#2207)
 * [c5afc9069](https://github.com/numaproj/numaflow/commit/c5afc906995c6cf2b4f0fa8f4b9c7d8c54577f5f) feat: use sidecar for ud containers [Breaking k8s < v1.29] (#2230)
 * [4f2568ba4](https://github.com/numaproj/numaflow/commit/4f2568ba451fdb2cafdf13f5152689d37b03cb5b) chore(deps): bump cross-spawn from 7.0.3 to 7.0.6 in /ui (#2228)
 * [d507e19ff](https://github.com/numaproj/numaflow/commit/d507e19ff76eb0e68db33434802216f633a4763d) feat: add sdk infomation metrics (#2208)
 * [8f7132ddc](https://github.com/numaproj/numaflow/commit/8f7132ddc3e71928a33bfd03bf39318bbe1ea666) doc: roadmap update (#2217)

### Contributors

 * Adarsh Jain
 * Derek Wang
 * Martin Thøgersen
 * RohanAshar
 * SaniyaKalamkar
 * Sidhant Kohli
 * Sreekanth
 * SzymonZebrowski
 * Takashi Menjo
 * Takuma Watanabe
 * Vedant Gupta
 * Vigith Maurice
 * Yashash H L
 * dependabot[bot]
 * shrivardhan

## v1.4.6 (2025-05-18)

 * [b966b1bbc](https://github.com/numaproj/numaflow/commit/b966b1bbc88337fcec8580dea3417fbc3710ce2e) Update manifests to v1.4.6

### Contributors

 * Derek Wang

## v1.4.5 (2025-05-13)

 * [9090935aa](https://github.com/numaproj/numaflow/commit/9090935aafcaf6fc173bc27312c2a09bef22cec4) Update manifests to v1.4.5
 * [62c456099](https://github.com/numaproj/numaflow/commit/62c4560996e384657009d2230ca925ffec60499b) feat: expose metrics for pipeline/mvtx desired and current phase (#2563)
 * [6e49fb114](https://github.com/numaproj/numaflow/commit/6e49fb114361f6c451ffd4ab4beac40b63aae259) fix: broken retry strategy in pipeline sink (#2625)

### Contributors

 * Derek Wang
 * Yashash H L

## v1.4.4 (2025-04-03)

 * [ae9d93ad0](https://github.com/numaproj/numaflow/commit/ae9d93ad0aaaf61854696b1638fd7d813dda7cc8) Update manifests to v1.4.4
 * [92f169e60](https://github.com/numaproj/numaflow/commit/92f169e608a9b541d0b43c27697541c108bc3ac0) fix: udsink handler for multi partitions
 * [a3a9fe515](https://github.com/numaproj/numaflow/commit/a3a9fe515d63fb371036e6c8c3ef26ea1a102521) fix: rater tests
 * [82ef07e8e](https://github.com/numaproj/numaflow/commit/82ef07e8ec1d44809ee95ac9cfd493800b3c5307) fix: concurrent invocations to update active pods list in rater (#2478)
 * [b45cd3f81](https://github.com/numaproj/numaflow/commit/b45cd3f8140ce7a5d6c2f9d66c814dedab3c551a) fix: create a new bidi stream for every partition to avoid race condition in sink (#2517)

### Contributors

 * Adarsh Jain
 * Yashash H L

## v1.4.3 (2025-02-28)

 * [00639161a](https://github.com/numaproj/numaflow/commit/00639161a39ef8a11d15fbe3ff3246472b005d36) fix(ci): upgrade deprecated runner (#2437)
 * [86cfd5a8c](https://github.com/numaproj/numaflow/commit/86cfd5a8ced015d1dda529cc01d3bda02a1d86c5) Update manifests to v1.4.3
 * [776781cb9](https://github.com/numaproj/numaflow/commit/776781cb9f977f89f0b4a622bc460d18b90a7d73) fix(ci): actions/upload/download-artifact upgrade
 * [868cc5b2e](https://github.com/numaproj/numaflow/commit/868cc5b2e1b8a09713c221bbd645ce4290771593) chore(deps): bump github.com/go-jose/go-jose/v4 from 4.0.1 to 4.0.5 (#2428)
 * [3b9215546](https://github.com/numaproj/numaflow/commit/3b921554682af1b663f451aefe8ceb106bffebc8) refactor: introducing lifecycle to Vertex spec (#2419)
 * [24882b3e4](https://github.com/numaproj/numaflow/commit/24882b3e4953891bfa97c3dd907439da806a630c) fix(controller): replicas calculation for paused pipeline (#2412)
 * [a7741f3c7](https://github.com/numaproj/numaflow/commit/a7741f3c73e9cab7bba33e5be87bcdd86939ea24) fix: honor lookbackSeconds in monovertex and rust pipeline (#2258)
 * [e636ada2e](https://github.com/numaproj/numaflow/commit/e636ada2ebbc1fd9c9e64fac6db278fd603cde17) feat: Added support for kafka OAuth authentication (#2257)
 * [64c019dcd](https://github.com/numaproj/numaflow/commit/64c019dcd9268918425b9d2cb7d1794ec1ae5218) fix(controller): multiple issues related to min/max of a vertex (#2398)
 * [4098fab5b](https://github.com/numaproj/numaflow/commit/4098fab5b1adf8bec85fcba5049de51bdd9748ed) fix: more criteria to detect lifecycle changes, fixes: #2331 (#2346)
 * [8b2871a8c](https://github.com/numaproj/numaflow/commit/8b2871a8c4a6b69bd21f47ee221bf2ce1a138b35) chore(deps): bump golang.org/x/net from 0.29.0 to 0.34.0 (#2339)
 * [d618aa9f6](https://github.com/numaproj/numaflow/commit/d618aa9f6eed5e1fe1c4f0b9713a3b701f23b0c5) fix: clean up metrics for deleted mvtx (#2338)
 * [35273adc1](https://github.com/numaproj/numaflow/commit/35273adc1f15639e0d30295a5132ea932fe04e44) fix: lastScaledAt not updated during autoscaling down (#2323)
 * [1170f2ea9](https://github.com/numaproj/numaflow/commit/1170f2ea984ea5364cd08844bdbae29018df23c7) chore(deps): bump golang.org/x/crypto from 0.27.0 to 0.31.0 (#2290)
 * [bff0d2d5a](https://github.com/numaproj/numaflow/commit/bff0d2d5aa4d24c96086676988f2d7962f6f15ff) fix: sorting metrics chart tooltip values (#2401)
 * [7b1b2425a](https://github.com/numaproj/numaflow/commit/7b1b2425ae1ed2455975216f64923d2608734834) fix: filter out deamon pod and unnecessary containers from metrics output (#2394)
 * [52c200f6f](https://github.com/numaproj/numaflow/commit/52c200f6fa9ea1c897068c546b378cb775070c55) fix: error state & search/negate search in case of prev terminated logs (#2391)
 * [82a80c704](https://github.com/numaproj/numaflow/commit/82a80c7041a3a35dec3aa76d5bfd8563efcee1cd) feat: updating chart lables and tooltip (#2387)
 * [8eaa47877](https://github.com/numaproj/numaflow/commit/8eaa478771547642d7df0e3cf790f6abe6add290) feat: contextual flow for remaining metrics (#2379)
 * [a5f2a5a87](https://github.com/numaproj/numaflow/commit/a5f2a5a87b19cc36715d04bf07ac4f6a7e6c6f46) feat: cpu memory metrics (#2332)
 * [4d73e6fe7](https://github.com/numaproj/numaflow/commit/4d73e6fe7a2a295c1a6a1c967ae0483b1c0e0f89) feat: moving ud containers logs as default in pods view (#2378)
 * [ce5ff7ae7](https://github.com/numaproj/numaflow/commit/ce5ff7ae70753e2ae8d88cf79f0e15a40d856a98) fix: timestamp and level filter fix for logs (#2374)
 * [8ced82dfa](https://github.com/numaproj/numaflow/commit/8ced82dfabb5695b9cb51766931edffa73d624ab) fix: get status of init containers in pods-info API call (#2371)
 * [b1c9a9534](https://github.com/numaproj/numaflow/commit/b1c9a9534fdea531cadadb0794469b2d7ed22704) feat: rows/page dropbox. Fixes #1764 (#2337)
 * [8472c4d48](https://github.com/numaproj/numaflow/commit/8472c4d489b3cace6a05516f6c5c811c65caf4f9) fix: units and tooltip content for metric charts (#2333)
 * [01d422c4a](https://github.com/numaproj/numaflow/commit/01d422c4a48e27a5bb643d73c3c2cf66e5eea260) feat: contextual flow for metrics (#2345)
 * [dbb0b8be9](https://github.com/numaproj/numaflow/commit/dbb0b8be90739b788a1b8adb26159d88b71c42b5) feat: logs enhancements (#2320)
 * [26d925a37](https://github.com/numaproj/numaflow/commit/26d925a371d1fd1bbb054099eecc168906b9878a) feat: make unit and display name configurable (#2269)
 * [846c469d6](https://github.com/numaproj/numaflow/commit/846c469d64663f704326a7e3fb903ef0ee05fc3d) feat: enhance time range selector for metrics - include presets (#2292)
 * [3baa00b35](https://github.com/numaproj/numaflow/commit/3baa00b35fc2017f7c8102ca885ec4bab77a97df) chore(deps): bump nanoid from 3.3.7 to 3.3.8 in /ui (#2289)
 * [c8ef52183](https://github.com/numaproj/numaflow/commit/c8ef5218370ca6f7e985420ca4ae96aaf99a1fab) feat: pipeline gauge metrics (#2284)
 * [14712cd20](https://github.com/numaproj/numaflow/commit/14712cd20b3507eb35ca1029f081893fc41b41e1) feat: counter metrics visualizer for mono-vertex (#2256)
 * [0eaec8c81](https://github.com/numaproj/numaflow/commit/0eaec8c81103114cebd1e78c6be099ff4e055455) feat: mvtx gauge metrics support (#2259)
 * [4db11c9d9](https://github.com/numaproj/numaflow/commit/4db11c9d9dcfb56d4f78023ee227884f69fdbc2a) fix: "Loading Pods View" and "failed to get pod details" state on UI (#2248)
 * [e4d920cc5](https://github.com/numaproj/numaflow/commit/e4d920cc59ed374f1f6f63054b650526addd7a21) feat: counter metrics visualizer for pipeline (#2238)
 * [f9bfa4ee4](https://github.com/numaproj/numaflow/commit/f9bfa4ee40385559df24f184a404e1933f25d82a) fix: init container logs (#2245)
 * [bca18d308](https://github.com/numaproj/numaflow/commit/bca18d308a4609e50e31f841da28e28043311f00) feat: unify metrics ( cleanup and add missing metrics ) (#2207)
 * [ca182eb27](https://github.com/numaproj/numaflow/commit/ca182eb27430ff859322b4eca713a8c8381f6ae9) chore(deps): bump cross-spawn from 7.0.3 to 7.0.6 in /ui (#2228)

### Contributors

 * Adarsh Jain
 * Derek Wang
 * SaniyaKalamkar
 * Sidhant Kohli
 * SzymonZebrowski
 * Vedant Gupta
 * dependabot[bot]

## v1.4.2 (2024-12-11)

 * [c9dc38f4c](https://github.com/numaproj/numaflow/commit/c9dc38f4cce2b5db598536a7539f2a35febcf1ca) Update manifests to v1.4.2
 * [fea792b36](https://github.com/numaproj/numaflow/commit/fea792b36bd342adcdcdd96768b6fdd68921bfd2) fix: set max decode size of proto message (#2275)

### Contributors

 * Sidhant Kohli

## v1.4.1 (2024-12-05)

 * [346f2a732](https://github.com/numaproj/numaflow/commit/346f2a7321d158fa9ce9392cfdcc76d671d6f577) Update manifests to v1.4.1
 * [1343e4d47](https://github.com/numaproj/numaflow/commit/1343e4d47934afcea324d4426df810dd9e99d9ab) feat: add sdk infomation metrics (#2208)
 * [1abb5ede3](https://github.com/numaproj/numaflow/commit/1abb5ede3577016b7c2a923755e1445146efdb05) fix: Fix Sink Config to respect Fallback (#2261)

### Contributors

 * Derek Wang
 * Yashash H L

## v1.4.0 (2024-11-08)

 * [6892c1159](https://github.com/numaproj/numaflow/commit/6892c11590ea482c186724e55837dbcfb2100ce3) Update manifests to v1.4.0
 * [63d5f774f](https://github.com/numaproj/numaflow/commit/63d5f774fecc0284ea92ad3934e7c2c8e4a58b6e) feat: metrics visualiser for mono vertex (#2195)
 * [9c1d3cef6](https://github.com/numaproj/numaflow/commit/9c1d3cef6ca817f0e0595dc07b727ce8ae597e4e) feat: block isbsvc deleting when there is linked pipeline (#2202)
 * [426141a5e](https://github.com/numaproj/numaflow/commit/426141a5e595e8cb4c827f48fea0e1bd286e4a11) fix(docs): use manifests from main branch in quick-start (#2197)
 * [00a74df0f](https://github.com/numaproj/numaflow/commit/00a74df0f03a8548b3f11776c598c61266801706) doc: monovertex (#2193)
 * [eca3b0c0b](https://github.com/numaproj/numaflow/commit/eca3b0c0be314939422ee18cdc938546d3b9e4e3) feat:KafkaSource supports KafkaVersion modification (#2191)
 * [5b7778260](https://github.com/numaproj/numaflow/commit/5b7778260c85c74fc73bf098d5d4609d2f8e2a42) feat: source and sink implementation in Rust (blocking implementation) (#2190)
 * [e98ff9805](https://github.com/numaproj/numaflow/commit/e98ff980577ee8c161d41fb3b00fcda6db20c9e7) chore(deps): bump http-proxy-middleware from 2.0.6 to 2.0.7 in /ui (#2188)
 * [8e98c0854](https://github.com/numaproj/numaflow/commit/8e98c0854bc3c17626238b2c58326cac5a602a05) fix: refine vertex/mvtx pod clean up logic (#2185)
 * [ee27af35a](https://github.com/numaproj/numaflow/commit/ee27af35aa7920d26068e4c03cb6efdf874f08fc) fix(metrics): fix incorrect metric label and add docs (#2180)
 * [f21e75bcf](https://github.com/numaproj/numaflow/commit/f21e75bcf1e133d26eed83cec9983501f3648ae3) fix(controller): incorporate instance into lease lock name (#2177)
 * [7b02290d3](https://github.com/numaproj/numaflow/commit/7b02290d3c8ee665916625fb119490192b4560bd) feat: config management for numaflow rust (#2172)
 * [187398ccd](https://github.com/numaproj/numaflow/commit/187398ccd1569316ad7303cdc86f7faed98e1eb1) fix: main branch, offset type got updated (#2171)
 * [dc137c24b](https://github.com/numaproj/numaflow/commit/dc137c24b3cc842c8a3e048fa928bc2a54f4d759) feat: blackhole sink for Monovertex (#2167)
 * [9bd7e1b29](https://github.com/numaproj/numaflow/commit/9bd7e1b2925ad8714d86114618fe93f967d2b7fe) feat: check if the buffer is full before writing to ISB (#2166)
 * [3d6e47ffc](https://github.com/numaproj/numaflow/commit/3d6e47ffc119d8347a2087fb951f2061c516bc94) feat: ISB(jetstream) writer framework (#2160)
 * [8bf96793a](https://github.com/numaproj/numaflow/commit/8bf96793aa477d85d31dac01edc36c9201f55fc2) fix: create histogram buckets in a range (#2144)
 * [c95d93083](https://github.com/numaproj/numaflow/commit/c95d930830912ceef3516b46994508c56214d236) feat: support multiple controller with instance config (#2153)
 * [1ea4d2ea3](https://github.com/numaproj/numaflow/commit/1ea4d2ea3f4a7b2ab939976eba5308d6cb0a9da0) feat: Log sink implementation for Monovertex (#2150)
 * [6fb36acfc](https://github.com/numaproj/numaflow/commit/6fb36acfc31f07bd53bfadf587fd2253dda9fe34) feat: Unify MapStream and Unary Map Operations Using a Shared gRPC Protocol (#2149)
 * [fb328854d](https://github.com/numaproj/numaflow/commit/fb328854d8a49aa915aaf7d3843ebcfdfd6c81a9) feat: actor pattern for forwarder + sink trait (#2141)
 * [bc12925f5](https://github.com/numaproj/numaflow/commit/bc12925f550d05732a435581570d6e1c0948f377) feat: set kafka keys if setKey is set (#2146)
 * [dd08bcab1](https://github.com/numaproj/numaflow/commit/dd08bcab15c7dad09930cb158b8b98caa3698d0e) feat: Unify Batch Map and Unary Map Operations Using a Shared gRPC Protocol (#2139)
 * [271e459a5](https://github.com/numaproj/numaflow/commit/271e459a5deb13f77906fb58c8308151ef6415a1) feat: add keys into kafka header while producing (#2143)
 * [206ff7f72](https://github.com/numaproj/numaflow/commit/206ff7f72bf83e19edf17eb36861865585b1ce9c) fix: pipeline pausing race conditions of draining and terminating source (#2131)
 * [d340a4e83](https://github.com/numaproj/numaflow/commit/d340a4e83311d487c2a1b3a75447168a01e3943e) feat: expose ports for user defined containers (#2135)
 * [fae53fa2d](https://github.com/numaproj/numaflow/commit/fae53fa2dcef7ef18d0d068368aaf6b832410c1e) feat: integrate tickgen with monovertex (#2136)
 * [d5c96fd95](https://github.com/numaproj/numaflow/commit/d5c96fd9538d6eebe7267f89f677c837b309d6a0) feat: Use gRPC bidirectional streaming for map  (#2120)
 * [ceb8f5b72](https://github.com/numaproj/numaflow/commit/ceb8f5b721c097310a5b91d89d1bd3df8648f284) feat: Make Generator Support Leaky Bucket (#2129)
 * [fcef50536](https://github.com/numaproj/numaflow/commit/fcef50536cf85373e0ba8ac5162a27fd4e58db5f) refactor: generate static gRPC clients (#2128)
 * [3182db3af](https://github.com/numaproj/numaflow/commit/3182db3af3a13ace6e7c2cb26d3be9df04173a5a) feat: generator based on ticker (#2126)
 * [06515a2cb](https://github.com/numaproj/numaflow/commit/06515a2cbfc3a183131cab54394bb5d2c546e046) fix: create buffers and buckets before updating Vertices (#2112)
 * [7586ffb05](https://github.com/numaproj/numaflow/commit/7586ffb056f3155fd9f13ba8dee33be38851ce94) Debugging unit test timeout in CI (#2118)
 * [dc25c4dc1](https://github.com/numaproj/numaflow/commit/dc25c4dc11c7fd5125c53bfff2b39fa49b9c8368) feat: implement Source trait and use it for user-defined source (#2114)
 * [3dbed43ea](https://github.com/numaproj/numaflow/commit/3dbed43ea652ed5e4913e2346e60816d52b258ed) feat: container-level version compatibility check for monovertex (#2108)
 * [6aacb6ea8](https://github.com/numaproj/numaflow/commit/6aacb6ea8bf656c1d65888deba4c21b0aea5de73) chore(deps): bump tonic from 0.12.2 to 0.12.3 in /rust (#2111)
 * [e69551ba0](https://github.com/numaproj/numaflow/commit/e69551ba07d14dee5dccd90b28cf8b497943f415) feat: Use gRPC bidirectional streaming for source transformer (#2071)
 * [5b8b8ddda](https://github.com/numaproj/numaflow/commit/5b8b8dddac727e53bcfbf4c8071221b284e606e9) chore(deps): bump rollup from 2.79.1 to 2.79.2 in /ui (#2096)
 * [6cdec2d6d](https://github.com/numaproj/numaflow/commit/6cdec2d6d1325866e99a204389bc9dc460146cbf) feat: Bidirectional Streaming for UDSink (#2080)
 * [895a77804](https://github.com/numaproj/numaflow/commit/895a7780410b7bb5a43a4ab6f4dd55c1c145561f) feat: container-type level version compatibility check (#2087)
 * [6d1ebd04f](https://github.com/numaproj/numaflow/commit/6d1ebd04f2089c81bd8e0c5e763cd7c363cb7623) feat: add pause for monovertex (#2077)
 * [b4f927857](https://github.com/numaproj/numaflow/commit/b4f9278570f67cba3d85fffe7ca287c5b00da489) fix: rollback codegen script (#2079)
 * [40e960a44](https://github.com/numaproj/numaflow/commit/40e960a44184c876173e6bdf69b216df6296bf73) feat: Bidirectional Streaming for User Defined Source (#2056)
 * [669dc186a](https://github.com/numaproj/numaflow/commit/669dc186a0d885df92716b627ded236fab7476e7) Fix: Use Merge patch rather than json patch for `pause-timestamp` annotation apply (#2078)
 * [ed543ad2e](https://github.com/numaproj/numaflow/commit/ed543ad2e7824f3e6b508de5b07ba08e1d7d9b66) fix: support version compatibility check for pre-release versions (#2069)
 * [9995ff813](https://github.com/numaproj/numaflow/commit/9995ff813d39489d22c94e574adae9e6a8a4ebe8) feat: allow customization on readyz and livez config (#2068)
 * [cbe9054f8](https://github.com/numaproj/numaflow/commit/cbe9054f8507639dac3a48b7b8eeb9e236ce706e) doc: example for PVC (#2067)
 * [692fbeec1](https://github.com/numaproj/numaflow/commit/692fbeec1b94d8ff66a82b9c3fe5d8242962750b) fix: skip updating phase for resource check (#2065)
 * [c6003314c](https://github.com/numaproj/numaflow/commit/c6003314c8f77905fbd86ddccab12853ca6c63a1) chore(deps): bump express from 4.19.2 to 4.21.0 in /ui (#2061)
 * [0811eb4af](https://github.com/numaproj/numaflow/commit/0811eb4aff59dda8b9143a7420b2beb415143d27) fix: Fix numaflow-rs binary location in image (#2050)
 * [ba40b1500](https://github.com/numaproj/numaflow/commit/ba40b1500416a258fe131273d3cfc4b46a93a88f) fix: builtin transformer should keep the keys (#2047)
 * [c4b4d0068](https://github.com/numaproj/numaflow/commit/c4b4d0068012f06980595437b3bc39c73cace8ef) feat: rolling update for Pipeline Vertex (#2040)
 * [328788776](https://github.com/numaproj/numaflow/commit/3287887761fa5a8da12ca70c5ce53947cbe896ec) feat: rolling update for MonoVertex (#2029)
 * [cf90e2582](https://github.com/numaproj/numaflow/commit/cf90e258261b50d95db2787cfe23e9008c2ab72a) fix: pause lifecyle changes and add drained status (#2028)
 * [40a3d2f5b](https://github.com/numaproj/numaflow/commit/40a3d2f5bd3ac57e075bc23b076c1e5df8436fc8) feat: allow configurable retryStrategy (#2010)
 * [55230e84f](https://github.com/numaproj/numaflow/commit/55230e84fd86f05bcac96dd4b42afe73aa1b2e4a) chore(deps): bump webpack from 5.93.0 to 5.94.0 in /ui (#2018)
 * [a77c9391e](https://github.com/numaproj/numaflow/commit/a77c9391e9e6dbdd00cbc50376b90b99eebc6cc5) fix: add latency metrics for mvtx (#2013)
 * [35c6f0991](https://github.com/numaproj/numaflow/commit/35c6f0991d6821b728c82bee6161e265dc2c1ba6) feat: introduce `readyReplicas` for Vertex and MonoVertex (#2014)
 * [2ba54117d](https://github.com/numaproj/numaflow/commit/2ba54117d7015126c6894d196d42848bd2e37644) feat: enable resourceClaims for vertex and monovtx (#2009)
 * [53d1131d8](https://github.com/numaproj/numaflow/commit/53d1131d82c8029e546c2f39305d1bcf80f1b60e) fix: log format with config load error (#2000)
 * [91f372ca9](https://github.com/numaproj/numaflow/commit/91f372ca9ea413041ad157746530481d78114fcf) feat: more flexible scaling with `replicasPerScaleUp` and `replicasPerScaleDown` (#2003)
 * [102d1de12](https://github.com/numaproj/numaflow/commit/102d1de1230a5a9baf29128757b12e6af4413bf3) chore(deps): bump micromatch from 4.0.7 to 4.0.8 in /ui (#2002)
 * [ae02243b3](https://github.com/numaproj/numaflow/commit/ae02243b3f30de8da407b148bbac7cb2e48a68c4) fix: e2e testing isbsvc deletion timeout issue (#1997)
 * [deb1626ec](https://github.com/numaproj/numaflow/commit/deb1626ece55579d30e6d9003abe854980cc2923) fix: test coverage generation for Rust code (#1993)
 * [6918e6f47](https://github.com/numaproj/numaflow/commit/6918e6f47e9309173dd67e6fc0c105d2cd9814f2) fix: do not pass scale info to MonoVertex (#1990)
 * [3f735f764](https://github.com/numaproj/numaflow/commit/3f735f76425a15d8670f145e69e3caa044037a2c) fix: adding not available for negative processing rates (#1983)
 * [33bbbad4d](https://github.com/numaproj/numaflow/commit/33bbbad4d7b16f9494d4164993b1cb9d32acc18b) fix: minor perf improvements of mvtx fallback sink (#1967)
 * [af2f65220](https://github.com/numaproj/numaflow/commit/af2f65220afa80fc8f4bf684cc9ce58234c2bb80) fix: remove coloring in logs (#1975)
 * [a7074aa80](https://github.com/numaproj/numaflow/commit/a7074aa80345e41c39770e7d069e14c29eaff9e0) doc: update roadmap (#1970)
 * [e1bfd1b2d](https://github.com/numaproj/numaflow/commit/e1bfd1b2d016d64bfb9d6ac546cc3489c96b806d) refactor: re-arrange e2e tests  (#1961)
 * [426711382](https://github.com/numaproj/numaflow/commit/42671138250d67f6eacddf33d4b5d5e069e5674f) fix: replicas derived in UI from mvtx status instead of spec (#1965)
 * [b54a4cd3e](https://github.com/numaproj/numaflow/commit/b54a4cd3e555ee3e29c603f7f2ea1c15ccd88f7a) feat: add health for monovertex (#1954)
 * [cbad6996f](https://github.com/numaproj/numaflow/commit/cbad6996f063acf1f4a3d2d8fc2ec1acff6ee912) feat: enable fallback sink for mvtx (#1957)
 * [c14abd5de](https://github.com/numaproj/numaflow/commit/c14abd5de5cfc4d88f396c17231233b4e9fc2c5f) feat: Mono vertex UI (#1941)
 * [c4b5d05c2](https://github.com/numaproj/numaflow/commit/c4b5d05c24c189684043688fa657295bf4495dcd) fix: default resources mutated when applying templates (#1948)
 * [9e9638677](https://github.com/numaproj/numaflow/commit/9e9638677a35384e9acd12a1ecca1390fdf72b3e) feat: autoscaling for MonoVertex (#1927)
 * [97f942838](https://github.com/numaproj/numaflow/commit/97f94283817f994549e8e5cb0b78bf9e8444eabf) fix: retry failed messages for MonoVertex sink (#1933)
 * [2017f0c0f](https://github.com/numaproj/numaflow/commit/2017f0c0f3a7fd3f7842fc70575644e38b69d294) Add Lockheed to Users.md (#1934)
 * [8b7a9a16e](https://github.com/numaproj/numaflow/commit/8b7a9a16e89bc5f81d36c1abb44201ad850c32bc) feat: add server-info support and versioning to MonoVertex (#1918)
 * [c399d0514](https://github.com/numaproj/numaflow/commit/c399d051466017dc331552531ea31d44a20bae66) feat: source to sink with an optional transformer without ISB (#1904)

### Contributors

 * Derek Wang
 * Julie Vogelman
 * Keran Yang
 * Sidhant Kohli
 * Sreekanth
 * Vedant Gupta
 * Vigith Maurice
 * Yashash H L
 * dependabot[bot]
 * mdwarne1
 * qianbeibuzui
 * xdevxy

## v1.3.4 (2025-02-28)

 * [31fa578b4](https://github.com/numaproj/numaflow/commit/31fa578b4667ddc18f3e7b5e966a89d0027eab3d) fix(ci): upgrade ubuntu runner
 * [27e061f7e](https://github.com/numaproj/numaflow/commit/27e061f7ecd69144e784f280a1b9cef0752a81e9) Update manifests to v1.3.4
 * [ac161dfb8](https://github.com/numaproj/numaflow/commit/ac161dfb83d57a6b066e6bb8599ba28c9b76ee15) chore(deps): security fixes
 * [e2f6951d5](https://github.com/numaproj/numaflow/commit/e2f6951d5cf2e0a83ad35d4335d0a3f7ae18d270) chore(deps): bump nanoid from 3.3.7 to 3.3.8 in /ui (#2289)

### Contributors

 * Derek Wang
 * adarsh0728
 * dependabot[bot]

## v1.3.3 (2024-10-09)

 * [4f31aad7f](https://github.com/numaproj/numaflow/commit/4f31aad7f51cce59700ef53f363d06afeb6d6aee) Update manifests to v1.3.3
 * [d01336364](https://github.com/numaproj/numaflow/commit/d01336364b1826c9d28ff81828919b17ca8da222) fix: pipeline pausing race conditions of draining and terminating source (#2131)
 * [688dd7304](https://github.com/numaproj/numaflow/commit/688dd73049617511806779a4e535ad9f380af21f) feat: expose ports for user defined containers (#2135)
 * [a4a4fd057](https://github.com/numaproj/numaflow/commit/a4a4fd0578f7a4e45a6435505d03061c3612ed6f) fix: create buffers and buckets before updating Vertices (#2112)
 * [498583f24](https://github.com/numaproj/numaflow/commit/498583f24573649f6ed2db959742f515804a2edc) chore(deps): bump rollup from 2.79.1 to 2.79.2 in /ui (#2096)

### Contributors

 * Derek Wang
 * Julie Vogelman
 * Sidhant Kohli
 * dependabot[bot]

## v1.3.2 (2024-09-26)

 * [cb7d17d4f](https://github.com/numaproj/numaflow/commit/cb7d17d4f3e2ecfcf6a1aa413031f714c135983d) Update manifests to v1.3.2
 * [816a8e749](https://github.com/numaproj/numaflow/commit/816a8e749c3f071b0c5e4c2ce97025c8138c6cbb) feat: container-type level version compatibility check (#2087)
 * [9fae141e6](https://github.com/numaproj/numaflow/commit/9fae141e686c99e95824b8bdcbec4d4e1bf04241) feat: add pause for monovertex (#2077)
 * [fc59a3e06](https://github.com/numaproj/numaflow/commit/fc59a3e06f3e86947d9f905e1a728aa155f68bf4) fix: rollback codegen script (#2079)
 * [82beda646](https://github.com/numaproj/numaflow/commit/82beda6462b4828f914ea70a07d8bed5f1302675) Fix: Use Merge patch rather than json patch for `pause-timestamp` annotation apply (#2078)
 * [9c2e8f812](https://github.com/numaproj/numaflow/commit/9c2e8f812148d6cc45781762b6328a509225e747) fix: support version compatibility check for pre-release versions (#2069)
 * [b0e60014b](https://github.com/numaproj/numaflow/commit/b0e60014b3e98019fcc20edd668de90e542534a3) feat: allow customization on readyz and livez config (#2068)
 * [88d2a7a30](https://github.com/numaproj/numaflow/commit/88d2a7a30ae9e1fd63799878f1a0e8b0650615e8) doc: example for PVC (#2067)
 * [7726cf427](https://github.com/numaproj/numaflow/commit/7726cf4272a2f534202e4ec6ecd81c52e731dbf7) fix: skip updating phase for resource check (#2065)
 * [782872f55](https://github.com/numaproj/numaflow/commit/782872f55c7fcfb0b1f4747ad71c71f0fc26280c) chore(deps): bump express from 4.19.2 to 4.21.0 in /ui (#2061)
 * [234e19fc8](https://github.com/numaproj/numaflow/commit/234e19fc84d8c4f3c29217ccc4deafb35e9182f1) fix: builtin transformer should keep the keys (#2047)
 * [f7716aa2a](https://github.com/numaproj/numaflow/commit/f7716aa2a6afd9cc3596d307c68125f77cf55f92) feat: rolling update for Pipeline Vertex (#2040)
 * [db9337a2c](https://github.com/numaproj/numaflow/commit/db9337a2cd00e0b19fda1f79793bb9f35eae9436) feat: rolling update for MonoVertex (#2029)
 * [6f3764140](https://github.com/numaproj/numaflow/commit/6f3764140cd86356a197c8d15cd6f7b7afc0a4a0) fix: pause lifecyle changes and add drained status (#2028)
 * [754bc5e36](https://github.com/numaproj/numaflow/commit/754bc5e3646f52ed0784bdfc9810f6ad77c5ae2d) fix: Fix numaflow-rs binary location in image (#2050)

### Contributors

 * Derek Wang
 * Julie Vogelman
 * Keran Yang
 * Sidhant Kohli
 * Sreekanth
 * Vigith Maurice
 * dependabot[bot]

## v1.3.1 (2024-09-02)

 * [a42d0063c](https://github.com/numaproj/numaflow/commit/a42d0063caf53d6f4c01c2fb2f6f6f6f74a8f987) Update manifests to v1.3.1
 * [6993e75f5](https://github.com/numaproj/numaflow/commit/6993e75f546f2ffc6db1ecbb0fc579a5d6048754) feat: allow configurable retryStrategy (#2010)
 * [6c9736987](https://github.com/numaproj/numaflow/commit/6c973698762488915df719161ec4a70a130b4bea) chore(deps): bump webpack from 5.93.0 to 5.94.0 in /ui (#2018)
 * [cd54e86f7](https://github.com/numaproj/numaflow/commit/cd54e86f7d42641182531df3823baecece0ee57c) fix: add latency metrics for mvtx (#2013)
 * [c6530d37e](https://github.com/numaproj/numaflow/commit/c6530d37efce9a1a7ffd153cde104180b2c0b287) feat: introduce `readyReplicas` for Vertex and MonoVertex (#2014)
 * [13c13e5f1](https://github.com/numaproj/numaflow/commit/13c13e5f1a36957b11219cac49ad8e872bd290be) feat: enable resourceClaims for vertex and monovtx (#2009)
 * [1040a0223](https://github.com/numaproj/numaflow/commit/1040a0223ad54ce619e6b33eeb5b99bf341d807d) fix: log format with config load error (#2000)
 * [8d2a4b21f](https://github.com/numaproj/numaflow/commit/8d2a4b21fe18085ed12303a604019dc88fca4665) feat: more flexible scaling with `replicasPerScaleUp` and `replicasPerScaleDown` (#2003)
 * [9e54b2cda](https://github.com/numaproj/numaflow/commit/9e54b2cdaa75f9679dac2f37a0a7df88a39b481f) chore(deps): bump micromatch from 4.0.7 to 4.0.8 in /ui (#2002)
 * [d841421f7](https://github.com/numaproj/numaflow/commit/d841421f7d09da448cae10a45fa91a3bf9013d5c) fix: e2e testing isbsvc deletion timeout issue (#1997)
 * [991bfb701](https://github.com/numaproj/numaflow/commit/991bfb701195ed2c6bfbc01f2ce8af99bfc5d763) fix: test coverage generation for Rust code (#1993)
 * [a39746c11](https://github.com/numaproj/numaflow/commit/a39746c118791a37725f41241da4b3a9a03fa5a5) fix: do not pass scale info to MonoVertex (#1990)
 * [0dcd9284d](https://github.com/numaproj/numaflow/commit/0dcd9284d6a46869d81281a7e267a59b51282148) fix: adding not available for negative processing rates (#1983)
 * [c49fdb9af](https://github.com/numaproj/numaflow/commit/c49fdb9af350b37aed7ef9b5b3d491cd85fe14a0) fix: minor perf improvements of mvtx fallback sink (#1967)
 * [24239fc1c](https://github.com/numaproj/numaflow/commit/24239fc1cc5a834621904cc12186b9d4dd51f950) fix: remove coloring in logs (#1975)
 * [26b0d1dbd](https://github.com/numaproj/numaflow/commit/26b0d1dbdba51da944604cbae11029727ee3b26e) doc: update roadmap (#1970)

### Contributors

 * Derek Wang
 * Keran Yang
 * Sidhant Kohli
 * Sreekanth
 * Vedant Gupta
 * Vigith Maurice
 * dependabot[bot]
 * xdevxy

## v1.3.0 (2024-08-19)

 * [4de121c2c](https://github.com/numaproj/numaflow/commit/4de121c2c3b436ac51fba97c8ce5153afc5364c9) Update manifests to v1.3.0
 * [a566548b8](https://github.com/numaproj/numaflow/commit/a566548b8d1be71367dc01049086b5a685b610eb) refactor: re-arrange e2e tests  (#1961)
 * [a30649dec](https://github.com/numaproj/numaflow/commit/a30649decdf381950e54187c1633b0e27fe85cff) fix: replicas derived in UI from mvtx status instead of spec (#1965)
 * [5a22c3219](https://github.com/numaproj/numaflow/commit/5a22c321912df0a5e4d59d8027a1173acfe0079c) feat: add health for monovertex (#1954)
 * [1087e860c](https://github.com/numaproj/numaflow/commit/1087e860c2896a861fb5068f9416dac39a948b30) feat: enable fallback sink for mvtx (#1957)
 * [b5aa6ffba](https://github.com/numaproj/numaflow/commit/b5aa6ffba0466f87727354d6a069e8d6fb8e07ba) feat: Mono vertex UI (#1941)
 * [447cd3f47](https://github.com/numaproj/numaflow/commit/447cd3f47cfaf7856c191ee27815fc338cea1cf3) fix: default resources mutated when applying templates (#1948)
 * [5a5316207](https://github.com/numaproj/numaflow/commit/5a53162077305cea43e6bf9d23dd19805c8c8bb4) feat: autoscaling for MonoVertex (#1927)
 * [78468019b](https://github.com/numaproj/numaflow/commit/78468019b0a292749ab688b4a74af1149b43d540) fix: retry failed messages for MonoVertex sink (#1933)
 * [206a535f7](https://github.com/numaproj/numaflow/commit/206a535f7e86f860ed5597616b0e3b1d9ab93ec0) Add Lockheed to Users.md (#1934)
 * [c1d25acd0](https://github.com/numaproj/numaflow/commit/c1d25acd0d1722c6011ada6ccff06cd5dc8812be) feat: add server-info support and versioning to MonoVertex (#1918)
 * [292e3eae4](https://github.com/numaproj/numaflow/commit/292e3eae4537c6d497f1eb2de5f72d3f657b4360) feat: source to sink with an optional transformer without ISB (#1904)

### Contributors

 * Derek Wang
 * Keran Yang
 * Sidhant Kohli
 * Vedant Gupta
 * Vigith Maurice
 * Yashash H L
 * mdwarne1

## v1.3.0-rc1 (2024-08-08)

 * [179f59674](https://github.com/numaproj/numaflow/commit/179f59674a0a61eb7ae7cd7a83612f0eb7b3be7f) Update manifests to v1.3.0-rc1
 * [51cc125ea](https://github.com/numaproj/numaflow/commit/51cc125eaa1f10cd896b0b5e6a7f9142659b179f) feat: introducing MonoVertex (#1911)
 * [5e56a594c](https://github.com/numaproj/numaflow/commit/5e56a594c8f6f23a4228ff0d740b6666e9f049a4) feat: Rust k8s model for Numaflow (#1898)
 * [bc1451a35](https://github.com/numaproj/numaflow/commit/bc1451a35f3871c3459955956eaa35f48abea761) feat: Add ObservedGeneration field in vertex status and use it for calculating status (#1892)
 * [280b9bd3e](https://github.com/numaproj/numaflow/commit/280b9bd3edc95100791b2f23a1e3eb9930db675c) fix: configure discard policy for WorkQueue/Interest  (#1884)
 * [d2a67588e](https://github.com/numaproj/numaflow/commit/d2a67588e3c9f165fab708a962c873beed235e95) feat: Sync the health status of  ISBService, pipeline and vertex (#1860)
 * [51a21fa8a](https://github.com/numaproj/numaflow/commit/51a21fa8a4ba9c12ad3a14ddf6dab0ef53525d73) feat: expose replica metrics for ISB Service and Vertex (#1859)
 * [e4e5f1c89](https://github.com/numaproj/numaflow/commit/e4e5f1c89bfa8913cff948dc7ae08ad828250cae) feat: new edge path (#1864)
 * [c07425b50](https://github.com/numaproj/numaflow/commit/c07425b50b38f09a381c0673e9f344c5874aed7b) feat: pipeline and isbsvc resource health status metrics and detailed vertex type stats (#1856)
 * [d708ffb0c](https://github.com/numaproj/numaflow/commit/d708ffb0cb9c3f02e1be1fd3804ab9785b4fa2f1) feat: add controller and pipeline info metrics (#1855)
 * [0b7f5a389](https://github.com/numaproj/numaflow/commit/0b7f5a3894fcceda5ff49a509c3a50a897b0a47d) fix: api docs for jetstream service (#1851)
 * [1db0d093b](https://github.com/numaproj/numaflow/commit/1db0d093b77d843911dc3a8890026b8e13bbf98a) feat: use same server-info file for all map modes (#1828)
 * [ccfb8c2c0](https://github.com/numaproj/numaflow/commit/ccfb8c2c0f804df3bc09933b4f9dfeba184b55f7) feat: Serving Source (#1806)
 * [d620f1b16](https://github.com/numaproj/numaflow/commit/d620f1b16ec1bc9fd572c5106ed7093f82f417d2) feat: add ObservedGeneration to Pipeline and ISBService Status (#1799)
 * [6e4a681ff](https://github.com/numaproj/numaflow/commit/6e4a681ff260d95a08395ce5f70005c0c2c166d4) fix(#1832): scale down to >=min, but not 0 when there's direct back pressure (#1834)
 * [fa18f97dc](https://github.com/numaproj/numaflow/commit/fa18f97dc7fb9d611c3498b3cea2b2ecd8598b96) fix: should never scale down to < min (#1832)
 * [cea0783f6](https://github.com/numaproj/numaflow/commit/cea0783f660827daa2d36d413baab05114854a56) fix: value can be null (#1831)
 * [251e84f23](https://github.com/numaproj/numaflow/commit/251e84f23eae3c54573bd87799055db750e50314) feat: enable restful daemon client option for UX server (#1826)
 * [00619b671](https://github.com/numaproj/numaflow/commit/00619b671d636aac78fa2330cf334b014874fc18) feat: implement map batch (#1778)
 * [8cff6d1ff](https://github.com/numaproj/numaflow/commit/8cff6d1ff13bf4b3343084b737065fc03596a2f0) fix: save trait should accept Self as mutable (#1795)
 * [2b0ac547e](https://github.com/numaproj/numaflow/commit/2b0ac547e70209b87a3868684035265ca6dd619f) feat: crate for retry with backoff strategy (#1785)
 * [5f3766ab9](https://github.com/numaproj/numaflow/commit/5f3766ab9173d4b7a510c881f4d655089eebb504) feat: use protobuf to store wmb in KV (#1782)
 * [215333931](https://github.com/numaproj/numaflow/commit/21533393125c8a2754f74f689ef6bf2f66c59830) feat: use protobuf to store header and messages in ISB (#1771)
 * [07483c855](https://github.com/numaproj/numaflow/commit/07483c85540fb30f605a8d018ff4aa42ff7ac01d) fix: add retries when writing to redis and resp headers const (#1766)
 * [1fc41e939](https://github.com/numaproj/numaflow/commit/1fc41e93991807cd4170474f9b3ec0b4dccb1a6a) feat: serving and tracking endpoint for Numaflow (#1765)
 * [8da7c2296](https://github.com/numaproj/numaflow/commit/8da7c2296b11d717cb5911256bd9d11af10b4ac1) feat: publish to callback endpoint for tracking (#1753)
 * [f69d83032](https://github.com/numaproj/numaflow/commit/f69d8303268f79bdeaaa5603f44bbd38e0913e53) chore(deps): bump ws from 7.5.9 to 7.5.10 in /ui (#1762)
 * [0f91c7ef8](https://github.com/numaproj/numaflow/commit/0f91c7ef8e52819f096a23b1f2759b014952988e) chore(deps): bump braces from 3.0.2 to 3.0.3 in /ui (#1758)
 * [b26008e84](https://github.com/numaproj/numaflow/commit/b26008e8414d99bda71ec23f5c1dd3770bf63972) feat(config): standardize boolean value with YAML tags. Fixes #1742 (#1749)
 * [71bc030dc](https://github.com/numaproj/numaflow/commit/71bc030dcea12ec97758b9023ba567744666c09a) feat: adding numaflow version to the UI (#1744)
 * [1e03eee4f](https://github.com/numaproj/numaflow/commit/1e03eee4f2ea123310db02d89775e20f85906d0f) fix: update SDKs to stable image (#1746)
 * [30c42f635](https://github.com/numaproj/numaflow/commit/30c42f635c3d9840e1a3f5acdfed1d1a3d33e0be) doc: update roadmap (#1748)
 * [61a17aafa](https://github.com/numaproj/numaflow/commit/61a17aafa82219ddb17b1b40a3c39e297c7f6584) Tick generator blob - Closes #1732 (#1733)
 * [3759cde08](https://github.com/numaproj/numaflow/commit/3759cde082f273d2bc126fa2f463d47a4eda09bd) fix: Read from Oldest Offset for Idle Source Kafka e2e  (#1731)
 * [0cd57bc4e](https://github.com/numaproj/numaflow/commit/0cd57bc4e6f54e3e1d756f59f67ec4720823f3a9) feat: Built-in Jetstream source implementation (Closes #1695) (#1723)
 * [d2580c6d8](https://github.com/numaproj/numaflow/commit/d2580c6d8bd5a83b56ca69cb0858af001f9b187b) fix: height fixes to render pipeline view (#1720)
 * [3d9358f9c](https://github.com/numaproj/numaflow/commit/3d9358f9c0670647bad76aa16ff5c8e188ff5a63) doc: add numaflow-controller-config link (#1719)
 * [1c7725766](https://github.com/numaproj/numaflow/commit/1c7725766ac1f4447536faf902d33c31983bda84) fix: summary bar overlay fix for plugin (#1710)
 * [7ab5788d2](https://github.com/numaproj/numaflow/commit/7ab5788d2478e6444b6212a761fda2793f417e4a) chore(deps): bump ejs from 3.1.9 to 3.1.10 in /ui (#1711)
 * [88b89bc0f](https://github.com/numaproj/numaflow/commit/88b89bc0f53268a687ae7c438d6c9f26dc10881c) doc: add "nav" for fallback-sink (#1694)
 * [6da279605](https://github.com/numaproj/numaflow/commit/6da279605e29d55523449b3d4d5df0391961d66c) doc: reduce streaming (#1689)
 * [aea4a329b](https://github.com/numaproj/numaflow/commit/aea4a329be26ef50566b08d00b4bdd11a5a07e17) doc: Fallback Sink (#1691)
 * [2f854b0d6](https://github.com/numaproj/numaflow/commit/2f854b0d6d2b249393fe94eccdc0c2780b2f451d) chore(deps): bump golang.org/x/net from 0.22.0 to 0.23.0 (#1692)
 * [9bfcf880f](https://github.com/numaproj/numaflow/commit/9bfcf880f2890885cd3e527b632d27f584c7edef) doc: session doc (#1650)
 * [685413587](https://github.com/numaproj/numaflow/commit/685413587e885213c3b036a8bcede198e04d0fa8) fix: version downgrade for monaco-editor  (#1673)
 * [db0d2ed1d](https://github.com/numaproj/numaflow/commit/db0d2ed1d9208cbe52123b3927f49fdfb30442ba) feat: Fallback Sink  (#1669)
 * [06ca9bc42](https://github.com/numaproj/numaflow/commit/06ca9bc42ad1a88f6cbd4cbb4b76c01cc325e48c) fix: routing fixes (#1671)
 * [3bb938204](https://github.com/numaproj/numaflow/commit/3bb938204de9c18925f1058a4ac3666498be64e9) feat: controller change for fallback sink (#1664)
 * [268b00d1d](https://github.com/numaproj/numaflow/commit/268b00d1ded263faf44d7c509c1d1b0e9d85998f) Enable cors for numaflow api (#1631)
 * [e9c3731bc](https://github.com/numaproj/numaflow/commit/e9c3731bcb53f5b5c18695efee2d2a4c5b616adc) feat: expose controller leader election duration and renew opts (#1657)
 * [e7cf8c773](https://github.com/numaproj/numaflow/commit/e7cf8c773372d3609c358c8aac0fa65eca4cbedf) fix: add headers to custom sinkrequest (#1653)
 * [2ef4286c9](https://github.com/numaproj/numaflow/commit/2ef4286c90a4e02631480d70d04ba250a4f426ee) fix: pass headers to transfomer (#1651)
 * [75195d56d](https://github.com/numaproj/numaflow/commit/75195d56dcc25bcbe277abe257c3d7eb7378ca86) fix: avoid publishing watermarks for duplicate messages. (#1649)
 * [872d8a839](https://github.com/numaproj/numaflow/commit/872d8a8399d9d782b9fe95929e029d158da999c1) fix: flaky TestDropOnFull  (#1647)
 * [645a69417](https://github.com/numaproj/numaflow/commit/645a694173a7d7a34cb7ad4e26d445e6de053886) fix: Dedup not working for multi-partitioned edge (#1639)
 * [a62970307](https://github.com/numaproj/numaflow/commit/a6297030792ca25435fb1c35f14c71e2b8daaf8b) fix: readonly view (#1640)
 * [0c68cd40a](https://github.com/numaproj/numaflow/commit/0c68cd40a3d4065b49e335a0a53d3c5f85925811) feat: read only view for UI (#1628)
 * [3dbba4f64](https://github.com/numaproj/numaflow/commit/3dbba4f64931313267cb9023c2e787bd86a111f6) fix: race condition while publishing wm inside reduce (#1599)
 * [74ab70aaf](https://github.com/numaproj/numaflow/commit/74ab70aaf992af370a5ffe779e1b3147263d84ad) fix: bug in late message handling for sliding window (#1471)
 * [35c2fe00a](https://github.com/numaproj/numaflow/commit/35c2fe00a0bef24a658ebecfc3a39de50c195ed0) fix: numaflow package style fixes (#1622)
 * [f1e5ba0eb](https://github.com/numaproj/numaflow/commit/f1e5ba0eb222edf3e5a5593769efc3626b092c1b) doc: add new user to the list (#1623)
 * [caf49c919](https://github.com/numaproj/numaflow/commit/caf49c9197398cdaac9c97c7d4e126b9eeeaeb19) fix: watermark progression during pods creation/deletion (#1619)
 * [756e66e6b](https://github.com/numaproj/numaflow/commit/756e66e6be14e2bd0d56d7a9a3fe48bb2aa1c385) fix: allow pipeline to start with redis isbsvc (Fixes: #1513) (#1567)
 * [ef94def97](https://github.com/numaproj/numaflow/commit/ef94def97157d1f532588f33504a17dd8e266623) fix: dedup in user defined source (#1613)
 * [c0b9fad21](https://github.com/numaproj/numaflow/commit/c0b9fad219de930de61a3bcb04cfff2bd60320c9) chore(deps): bump express from 4.18.2 to 4.19.2 in /ui (#1609)

### Contributors

 * Ali Ibrahim
 * Chandan Kumar
 * Charan
 * Derek Wang
 * Keran Yang
 * Matt Warner
 * Naga
 * Quentin FAIDIDE
 * Sidhant Kohli
 * Sreekanth
 * Vedant Gupta
 * Vigith Maurice
 * Yashash H L
 * dependabot[bot]
 * samhith-kakarla
 * xdevxy

## v1.2.2 (2024-11-15)

 * [61adf4e98](https://github.com/numaproj/numaflow/commit/61adf4e9805c2772d937a7513afcb3c14048127c) Update manifests to v1.2.2
 * [623cc4e2a](https://github.com/numaproj/numaflow/commit/623cc4e2aaa2d67d196cb972bd525a60544d2148) fix: update key len (#2223)

### Contributors

 * Sidhant Kohli

## v1.2.1 (2024-05-07)

 * [89ea33f1d](https://github.com/numaproj/numaflow/commit/89ea33f1d69785f6f5f17f1d5854ac189003918a) Update manifests to v1.2.1
 * [05610ad3e](https://github.com/numaproj/numaflow/commit/05610ad3e1a40915ad48c7fa62a8fee0b9235226) fix: height fixes to render pipeline view (#1721)

### Contributors

 * Vedant Gupta
 * Yashash H L

## v1.2.0 (2024-05-03)

 * [636ef873b](https://github.com/numaproj/numaflow/commit/636ef873b4a59e4350d4030ddcc7d86cf5400994) Update manifests to v1.2.0
 * [c9fc458be](https://github.com/numaproj/numaflow/commit/c9fc458be27b828e143db3fe5b4bb6668e0d1dde) fix: summary bar overlay fix for plugin (#1710)
 * [120244a3c](https://github.com/numaproj/numaflow/commit/120244a3c77192fdfbfb59e96a23d2d37a699154) chore(deps): bump ejs from 3.1.9 to 3.1.10 in /ui (#1711)

### Contributors

 * Vedant Gupta
 * Yashash H L
 * dependabot[bot]

## v1.2.0-rc5 (2024-04-24)

 * [2780c8e79](https://github.com/numaproj/numaflow/commit/2780c8e7992187e8b3c135df5d3d95321f418e8c) Update manifests to v1.2.0-rc5
 * [59e4b4538](https://github.com/numaproj/numaflow/commit/59e4b4538575c5ddbf59a5897014da8ae81e5c9c) doc: add "nav" for fallback-sink (#1694)
 * [df40e0894](https://github.com/numaproj/numaflow/commit/df40e0894f8811b8530c44f5d044c1f3feef37e6) doc: reduce streaming (#1689)
 * [8a6872e87](https://github.com/numaproj/numaflow/commit/8a6872e87e3e91eb380f14593d85e2236891a5a1) doc: Fallback Sink (#1691)
 * [746ddb0ab](https://github.com/numaproj/numaflow/commit/746ddb0abf4a26af65ab6dd3e32514e5bdd44577) chore(deps): bump golang.org/x/net from 0.22.0 to 0.23.0 (#1692)
 * [e21ac91ca](https://github.com/numaproj/numaflow/commit/e21ac91ca03bd4f58fd5aca019d713f08609bfd1) doc: session doc (#1650)

### Contributors

 * Derek Wang
 * Vigith Maurice
 * Yashash H L
 * dependabot[bot]

## v1.2.0-rc4 (2024-04-18)

 * [211bfacaa](https://github.com/numaproj/numaflow/commit/211bfacaad4a160d4e94a4dc139e6157b67e43d6) Update manifests to v1.2.0-rc4
 * [31e1a4985](https://github.com/numaproj/numaflow/commit/31e1a498518e2051627efdb9ca334c7dcf8025f9) fix: version downgrade for monaco-editor  (#1673)
 * [c8634256e](https://github.com/numaproj/numaflow/commit/c8634256ee6492af4ef7028ff980aeb7ae0a9828) feat: Fallback Sink  (#1669)
 * [d68a34685](https://github.com/numaproj/numaflow/commit/d68a34685b1acfd640298caa7eea23cbf4259c22) fix: routing fixes (#1671)
 * [680e5d4b6](https://github.com/numaproj/numaflow/commit/680e5d4b6e2e7729ad059ced90bace064f5b7ac3) feat: controller change for fallback sink (#1664)
 * [2faf759b1](https://github.com/numaproj/numaflow/commit/2faf759b1257a8d0f657f3fa1993c5c26b194518) Enable cors for numaflow api (#1631)
 * [6910744d0](https://github.com/numaproj/numaflow/commit/6910744d0d31c59d6a3b08c1ee47bc208a8aa772) feat: expose controller leader election duration and renew opts (#1657)

### Contributors

 * Ali Ibrahim
 * Derek Wang
 * Vedant Gupta
 * Yashash H L

## v1.2.0-rc3 (2024-04-09)

 * [4e172e808](https://github.com/numaproj/numaflow/commit/4e172e808d31659b2364088d8a07fcf6381d4040) Update manifests to v1.2.0-rc3
 * [109582c86](https://github.com/numaproj/numaflow/commit/109582c861a0603184e412602488df81eca8c474) fix: add headers to custom sinkrequest (#1653)
 * [6e12f09e4](https://github.com/numaproj/numaflow/commit/6e12f09e42efba50fe251dc1954a3d15ee29c1a8) fix: pass headers to transfomer (#1651)
 * [0b76352c5](https://github.com/numaproj/numaflow/commit/0b76352c5f1d557152f028df3166f098380c0bb8) fix: avoid publishing watermarks for duplicate messages. (#1649)
 * [1717e5115](https://github.com/numaproj/numaflow/commit/1717e511579fb78bc2e51c2da18ea4ef66e81afc) fix: flaky TestDropOnFull  (#1647)
 * [b4b21a5eb](https://github.com/numaproj/numaflow/commit/b4b21a5eb346d6b9d2be149b2c00f7dcbf66ec28) fix: Dedup not working for multi-partitioned edge (#1639)
 * [7a23eda07](https://github.com/numaproj/numaflow/commit/7a23eda07ae7fc8e7ea89af60af15813b67aae91) fix: readonly view (#1640)

### Contributors

 * Naga
 * Vedant Gupta
 * Yashash H L

## v1.2.0-rc2 (2024-04-03)

 * [66cc4903d](https://github.com/numaproj/numaflow/commit/66cc4903d480cd4b4eca73b829bfd78b483f4a95) Update manifests to v1.2.0-rc2
 * [a4c1d4803](https://github.com/numaproj/numaflow/commit/a4c1d4803d94226547db0d6db02e440aba933a93) feat: read only view for UI (#1628)
 * [45032fc60](https://github.com/numaproj/numaflow/commit/45032fc6070494f3e7c16e56b6a7bd0b376f2f2f) fix: race condition while publishing wm inside reduce (#1599)
 * [3e6cd333b](https://github.com/numaproj/numaflow/commit/3e6cd333bfbd20aac9de57432b0c3d85c94695e4) fix: bug in late message handling for sliding window (#1471)
 * [3d82431b4](https://github.com/numaproj/numaflow/commit/3d82431b4fc00e7010f7318266396ebc50e0a392) fix: numaflow package style fixes (#1622)
 * [73e434a97](https://github.com/numaproj/numaflow/commit/73e434a9742142da873fc9a7c27866a83d15ad45) doc: add new user to the list (#1623)
 * [f6ed4bbeb](https://github.com/numaproj/numaflow/commit/f6ed4bbeb0f163d0326ef07641ac0ecf64217b10) fix: watermark progression during pods creation/deletion (#1619)
 * [941fc674e](https://github.com/numaproj/numaflow/commit/941fc674e845170f9566e21e72622d175f266383) fix: allow pipeline to start with redis isbsvc (Fixes: #1513) (#1567)
 * [d1e10ffca](https://github.com/numaproj/numaflow/commit/d1e10ffca6887b24a41059293e1964c056d888da) fix: dedup in user defined source (#1613)
 * [e6b3d39c6](https://github.com/numaproj/numaflow/commit/e6b3d39c64fbcd0bf553e2493649787de5a1a399) chore(deps): bump express from 4.18.2 to 4.19.2 in /ui (#1609)

### Contributors

 * Quentin FAIDIDE
 * Vedant Gupta
 * Yashash H L
 * dependabot[bot]

## v1.2.0-rc1 (2024-03-26)

 * [0a1a2e8b8](https://github.com/numaproj/numaflow/commit/0a1a2e8b85f98e15d3e6a71eb8c07f591e796ea6) Update manifests to v1.2.0-rc1
 * [aab37c6c3](https://github.com/numaproj/numaflow/commit/aab37c6c3e18cee2ac43bc94f0ba2c1617038edd) fix: flaky e2e tests (#1590)
 * [84bafd0a1](https://github.com/numaproj/numaflow/commit/84bafd0a1eb98f8ae8a6e2e12c50023a454b8bd9) chore(deps): bump webpack-dev-middleware from 5.3.3 to 5.3.4 in /ui (#1595)
 * [04ff0c6e6](https://github.com/numaproj/numaflow/commit/04ff0c6e642c225607c03ccf23261e45215343fe) fix: error message that cause Buttons overflow in the UI (#1591)
 * [611bab700](https://github.com/numaproj/numaflow/commit/611bab7000fef3e6bd52b5a6a7fc84f78f87d8ae) feat: support headers for message (#1578)
 * [b394024fa](https://github.com/numaproj/numaflow/commit/b394024facea11499da1e5f3a469ac62bba4274d) feat: numaflow package (#1579)
 * [ea55a92dd](https://github.com/numaproj/numaflow/commit/ea55a92ddfa25a8591cbfe64ef21ad6ed7b1f37a) chore(deps): bump follow-redirects from 1.15.4 to 1.15.6 in /ui (#1573)
 * [4457ac881](https://github.com/numaproj/numaflow/commit/4457ac8819ed031e51c5f2b2def38a5e1f3c0a68) feat: read Kafka header and propagate in the payload (#1565)
 * [c1bc119cf](https://github.com/numaproj/numaflow/commit/c1bc119cfb4d4ab8836b3a79be1b02f5cb9e2ffb) fix(codegen): protobuf upgrade (#1558)
 * [4b580b148](https://github.com/numaproj/numaflow/commit/4b580b148c29ca12776eae134f31ec46563e8bcc) chore(deps): bump google.golang.org/protobuf from 1.31.0 to 1.33.0 (#1556)
 * [dd3cbfe35](https://github.com/numaproj/numaflow/commit/dd3cbfe35e9b0fbbff27782f8825affa2a7c546c) Chore: go 1.21 and k8s 1.29 (#1555)
 * [dc69b29b4](https://github.com/numaproj/numaflow/commit/dc69b29b4c5a91d18436de5db848549ac6cb2948) feat: unaligned wal (#1511)
 * [1844575f4](https://github.com/numaproj/numaflow/commit/1844575f4fbabd2205e1260740ff9b1c2fd9bb3d) fix: initialize inflightAcks channel to not nil channel (#1548)
 * [c6e5fd55a](https://github.com/numaproj/numaflow/commit/c6e5fd55a852a2c57c527f0987d58ccfd6bef8f5) chore(deps): bump github.com/go-jose/go-jose/v3 from 3.0.1 to 3.0.3 (#1549)
 * [cd05c47db](https://github.com/numaproj/numaflow/commit/cd05c47db27e607c42e819d56e10769a059b45d6) fix: break from retry loop when key is not found (#1535)
 * [fd3f5e1ad](https://github.com/numaproj/numaflow/commit/fd3f5e1adb2311d4164312959bf0ad163277af6e) feat: noop persistence store for reduce (#1532)
 * [8c2a160d5](https://github.com/numaproj/numaflow/commit/8c2a160d525e8fb455d730a67b48f4b535c1269b) fix(controller): vertex template metadata nil check (#1527)
 * [5b31bac5a](https://github.com/numaproj/numaflow/commit/5b31bac5afa5d67353feb368f8644a886b743fe1) feat: add support for SASL SCRAM 256 and 512 for Kafka (#1518)
 * [76266ef63](https://github.com/numaproj/numaflow/commit/76266ef6354a7fc9c506c13bcef70fe76a530a10) fix: incorrect json schema for tls config (#1520)
 * [762e130cd](https://github.com/numaproj/numaflow/commit/762e130cd34576d056e773db7a55eddbc3e0919a) fix: podSpec incorrectly configured in case template exist (#1516)
 * [37a9d5d79](https://github.com/numaproj/numaflow/commit/37a9d5d799156224efe00f8d0e7acdaf57eb2432) add separate server info file paths for services on client side (#1494)
 * [f05ce9e28](https://github.com/numaproj/numaflow/commit/f05ce9e28b458674f17ec1a9cc39799f3849069e) fix: idle manager refactor for multi partitions (#1512)
 * [c58f9a1bc](https://github.com/numaproj/numaflow/commit/c58f9a1bc9dd45c6d6bbf508f2ae6afb05be73fa) fix: avoid panic when ctx is canceled (#1515)
 * [abf7baf90](https://github.com/numaproj/numaflow/commit/abf7baf9095d9db1bb70eae2ef3f40bf9d00d1c2) fix: unknown for ISB details in pipeline card (#1497)
 * [20cf66d9e](https://github.com/numaproj/numaflow/commit/20cf66d9e2aed296b56ad0ffd534cc6c04b07674) feat: configure standardResources via controller configmap (#1490)
 * [345e7ca76](https://github.com/numaproj/numaflow/commit/345e7ca76c47a9f1468e9cbc66d90e3014f5bc8f) fix: add idle handler offset nil check (#1489)
 * [9954f8477](https://github.com/numaproj/numaflow/commit/9954f8477aaa64030616cf35c4e51ffdd66efda1) feat: terminate reduce vertex pods when pausing pipeline (#1481)
 * [0c53e8a03](https://github.com/numaproj/numaflow/commit/0c53e8a03c77d5caa833650015b57af640172220) fix(controller): incorrect cpu/mem resources calculation (#1477)
 * [495d22bf9](https://github.com/numaproj/numaflow/commit/495d22bf95304073b5f38f508131839b0381888b) feat: pipeline health status for UI (#1460)
 * [cd27ce602](https://github.com/numaproj/numaflow/commit/cd27ce602f33587fc6e54bdb9c5a1c50a75f5466) fix: bug where dashed line is treated as permalink + spelling/formatting (#1467)
 * [870d86bf1](https://github.com/numaproj/numaflow/commit/870d86bf1c7b32a00aa3d128bd19e32658425d59) feat: improve dex server (#1440)
 * [85e76c7c1](https://github.com/numaproj/numaflow/commit/85e76c7c16c82d9ab81cdddb7d463450da4ba1ee) fix: memory leak inside session windower (#1445)
 * [430635766](https://github.com/numaproj/numaflow/commit/430635766cec3298e0127e82d653719395e64e3a) fix: GetDownstreamEdges is not cycle safe (#1447)
 * [d1ad022e2](https://github.com/numaproj/numaflow/commit/d1ad022e2388ceec4a6b63426a601a2966731df9) chore(deps): bump follow-redirects from 1.15.3 to 1.15.4 in /ui (#1448)
 * [70c78e736](https://github.com/numaproj/numaflow/commit/70c78e7365fa86d6f3da51e896e238540d1db67a) fix: UI Filter by status for pipelines doesn't work as expected (#1444)
 * [795bef682](https://github.com/numaproj/numaflow/commit/795bef6828854b38925eeb4819344cd1ce93d35b) fix: Kafka source reads duplicated messages (#1438)
 * [7fe3225cb](https://github.com/numaproj/numaflow/commit/7fe3225cbb4d74d3c11775cbb8fb16d969c590cb) feat: enhance autoscaling peeking logic (#1432)
 * [58e215e17](https://github.com/numaproj/numaflow/commit/58e215e176137f6017ff9751891c06a0d0218a87) fix: server-secrets-init container restart  (#1433)
 * [9dc3bfd1d](https://github.com/numaproj/numaflow/commit/9dc3bfd1d2ac5705e8f834299f4432d15145e972) feat: update tcp client connections (#1429)
 * [45c85942c](https://github.com/numaproj/numaflow/commit/45c85942c312c0770695298c17623ba1bd323a1c) feat: Session Window and Reduce Streaming (#1384)
 * [38b44e69f](https://github.com/numaproj/numaflow/commit/38b44e69fe32dfb040aaa270415b4cc8a970455e) doc: idle source (#1426)
 * [bca1b3b9a](https://github.com/numaproj/numaflow/commit/bca1b3b9a16ad2cf202133fdc0970ef0a72762d4) feat: health status implementation (#1406)
 * [412bb210b](https://github.com/numaproj/numaflow/commit/412bb210b9ac07f851f001c153356f91058b0f8b) chore(deps): bump golang.org/x/crypto from 0.14.0 to 0.17.0 (#1424)
 * [5c7347265](https://github.com/numaproj/numaflow/commit/5c7347265e7bb45f82aac4501d45640997cce4f3) fix: configmap const name (#1423)

### Contributors

 * Abdullah Hadi
 * Ali Ibrahim
 * Antonino Fugazzotto
 * Damien RAYMOND
 * Derek Wang
 * Dillen Padhiar
 * Juanlu Yu
 * Nishchith Shetty
 * Sidhant Kohli
 * Vedant Gupta
 * Vigith Maurice
 * Yashash H L
 * akash khamkar
 * dependabot[bot]

## v1.1.7 (2024-03-15)

 * [d4e0bd285](https://github.com/numaproj/numaflow/commit/d4e0bd2854faf17705b575fb3e4afd97b3cf5094) Update manifests to v1.1.7
 * [e53f85840](https://github.com/numaproj/numaflow/commit/e53f85840c6b7c9e915b7b97986b8d74cdf3067b) fix: initialize inflightAcks channel to not nil channel (#1548)
 * [ae1ddaacd](https://github.com/numaproj/numaflow/commit/ae1ddaacdc1b3f3298ccadfee8cbf8e041a764db) chore(deps): bump github.com/go-jose/go-jose/v3 from 3.0.1 to 3.0.3 (#1549)

### Contributors

 * Antonino Fugazzotto
 * Derek Wang
 * dependabot[bot]

## v1.1.6 (2024-02-27)

 * [9613573e6](https://github.com/numaproj/numaflow/commit/9613573e63309b19acdd7ca8c257507ca89e7699) Update manifests to v1.1.6
 * [b8bb23363](https://github.com/numaproj/numaflow/commit/b8bb23363cd1b1596ce5c3c8374d3420f22dff25) fix(controller): vertex template metadata nil check (#1527)
 * [c0b3bdaca](https://github.com/numaproj/numaflow/commit/c0b3bdaca49e0aa26e7af3cc49200e7e3941dab4) feat: add support for SASL SCRAM 256 and 512 for Kafka (#1518)
 * [fe26f91a3](https://github.com/numaproj/numaflow/commit/fe26f91a3a6f8ae68759e10908ff028834420ffc) fix panic inside reduce after getting sigterm
 * [bdac5a75d](https://github.com/numaproj/numaflow/commit/bdac5a75dd0f46dc9dce24e0b82a2e6c425792b4) fix: incorrect json schema for tls config (#1520)
 * [53cfae972](https://github.com/numaproj/numaflow/commit/53cfae9726f77cef8250c5f649196b6f7f883ec7) fix: podSpec incorrectly configured in case template exist (#1516)
 * [915c09aa8](https://github.com/numaproj/numaflow/commit/915c09aa89dc82a1414df15fa2e9381c4bf98923) fix: unknown for ISB details in pipeline card (#1497)
 * [c71b2aba1](https://github.com/numaproj/numaflow/commit/c71b2aba159e90e773f1b6f3db2d1171424b8140) feat: configure standardResources via controller configmap (#1490)
 * [ce0a31558](https://github.com/numaproj/numaflow/commit/ce0a31558479ed0d69f0b9e182d28828a598fd8e) fix: add idle handler offset nil check (#1489)
 * [8d16d4994](https://github.com/numaproj/numaflow/commit/8d16d4994c91f1ecaaeee9b816eb616f49000111) feat: terminate reduce vertex pods when pausing pipeline (#1481)

### Contributors

 * Damien RAYMOND
 * Derek Wang
 * Dillen Padhiar
 * Juanlu Yu
 * Nishchith Shetty
 * Vedant Gupta
 * Yashash H L

## v1.1.5 (2024-01-23)

 * [e5bcf32e3](https://github.com/numaproj/numaflow/commit/e5bcf32e345454c577ecdd8a17e48e44d546730b) Update manifests to v1.1.5
 * [266cb2276](https://github.com/numaproj/numaflow/commit/266cb2276d29c3ddbfd5cdb5ce16f9a50da62042) fix(controller): incorrect cpu/mem resources calculation (#1477)

### Contributors

 * Derek Wang

## v1.1.4 (2024-01-20)

 * [7ffb521bc](https://github.com/numaproj/numaflow/commit/7ffb521bcc15612d04fe66de33d199e8c8391a7a) Update manifests to v1.1.4
 * [de780b95d](https://github.com/numaproj/numaflow/commit/de780b95da57437ceb4fc5d7bc77619c7e9deb2d) fix: bug in late message handling for sliding window (#1472)

### Contributors

 * Derek Wang
 * Yashash H L

## v1.1.3 (2024-01-14)

 * [0b96acf9b](https://github.com/numaproj/numaflow/commit/0b96acf9ba9b478e7284e6b5724822ac26a09ba8) Update manifests to v1.1.3
 * [907949be3](https://github.com/numaproj/numaflow/commit/907949be3da2295403d2367806c317be13452e68) fix: GetDownstreamEdges is not cycle safe (#1447)
 * [1d83b51e8](https://github.com/numaproj/numaflow/commit/1d83b51e8a9845a3b23e5ade83df2df599b0b9e4) chore(deps): bump follow-redirects from 1.15.3 to 1.15.4 in /ui (#1448)
 * [855672ddb](https://github.com/numaproj/numaflow/commit/855672ddb483931650bfc46e3075fe6d3a5c7998) fix: UI Filter by status for pipelines doesn't work as expected (#1444)
 * [c06de95eb](https://github.com/numaproj/numaflow/commit/c06de95eb56ce7c656496bfed5930a6c19db7555) fix: Kafka source reads duplicated messages (#1438)
 * [17c9c0e2e](https://github.com/numaproj/numaflow/commit/17c9c0e2e5dc4803bb2d1d977a441500bb5b771d) feat: enhance autoscaling peeking logic (#1432)

### Contributors

 * Derek Wang
 * Juanlu Yu
 * Nishchith Shetty
 * akash khamkar
 * dependabot[bot]

## v1.1.2 (2024-01-01)

 * [ac716ec4a](https://github.com/numaproj/numaflow/commit/ac716ec4ab4b49f4f013f067c33d1d89936e132a) Update manifests to v1.1.2
 * [af17d8ce2](https://github.com/numaproj/numaflow/commit/af17d8ce25665ddfe8e6eb65ed97c0a743eae4f4) fix: server-secrets-init container restart  (#1433)

### Contributors

 * Derek Wang
 * Vedant Gupta

## v1.1.1 (2023-12-21)

 * [5ff77fe0d](https://github.com/numaproj/numaflow/commit/5ff77fe0d6532ea5a513b7b94e6dea2af883ab2b) Update manifests to v1.1.1
 * [5fd20ad9a](https://github.com/numaproj/numaflow/commit/5fd20ad9a5c2a6d949e510633eb00887a3fa44da) chore(deps): bump golang.org/x/crypto from 0.14.0 to 0.17.0 (#1424)
 * [da32632c9](https://github.com/numaproj/numaflow/commit/da32632c9a0cb47e2b04d046471dddc3befb8766) fix: configmap const name (#1423)

### Contributors

 * Derek Wang
 * dependabot[bot]

## v1.1.0 (2023-12-18)

 * [07d46ca9d](https://github.com/numaproj/numaflow/commit/07d46ca9d0358db3625120328b428274ace54f2f) Update manifests to v1.1.0
 * [41b8dffc4](https://github.com/numaproj/numaflow/commit/41b8dffc46a5c46e8d8412c69ef291a8be3821da) feat: local user support for Numaflow (#1416)
 * [818be4f20](https://github.com/numaproj/numaflow/commit/818be4f20959ec8c9dd177039fe327ae6a22fb1f) fix: consider lastPublishedIdleWm when computed watermark is -1 (#1415)
 * [263263b30](https://github.com/numaproj/numaflow/commit/263263b30eee79a62588f791da0ce4295ff07877) fix: access path for auth endpoints (#1403)
 * [0db9cd191](https://github.com/numaproj/numaflow/commit/0db9cd191ebb9406aa5dc94a0766597bc3f03f03) feat: Generate Idle Watermark if the source is idling (#1385)
 * [6eb25c251](https://github.com/numaproj/numaflow/commit/6eb25c251263eb0d452100968ff9f8cf9b52382b) fix: include dropped messages in source watermark calculation (#1404)
 * [1eee1942d](https://github.com/numaproj/numaflow/commit/1eee1942d836c9cfd42417867b3126ea6eaebbf3) chore(deps): bump @adobe/css-tools from 4.3.1 to 4.3.2 in /ui (#1400)
 * [0a2ff5663](https://github.com/numaproj/numaflow/commit/0a2ff5663d6b15f9e0ec4c40d916003ed051f40e) fix: updated access path config for root path (#1397)
 * [024597d5b](https://github.com/numaproj/numaflow/commit/024597d5bb95144a48c91af1dd4a3c89173b4808) feat: improve numaflow k8s events (#1393)
 * [83fb90681](https://github.com/numaproj/numaflow/commit/83fb9068190e4f2ce8e09d8a41212d4848e597d6) fix: update numaflow-go version (#1387)
 * [9b0fadb6e](https://github.com/numaproj/numaflow/commit/9b0fadb6e4746e6acb232b2637c74dca60536c78) fix: access path for api/v1 route (#1388)
 * [96cfa5552](https://github.com/numaproj/numaflow/commit/96cfa55522ad8c7cfc4d8ddeac2e68ab964e919c) fix: dropped messages should not be considered for watermark propagation (#1386)
 * [5bf63076c](https://github.com/numaproj/numaflow/commit/5bf63076c45b0e95bd363d80855c4a880509b9f2) chore(deps): bump github.com/go-jose/go-jose/v3 from 3.0.0 to 3.0.1 (#1383)
 * [0c82ee0fd](https://github.com/numaproj/numaflow/commit/0c82ee0fd8ed296ba0750eefc554fcb2e1606d4a) refactor: move udf forwarder to the right dir (#1381)
 * [3a77ed42d](https://github.com/numaproj/numaflow/commit/3a77ed42d1ce48e41f0b3cca81cd9c940a1fc1c0) fix: add pipeline update validation checks (#1379)
 * [44a38f4a9](https://github.com/numaproj/numaflow/commit/44a38f4a9f411417d9abaa2aef82a9e9492edfd0) fix: disallow updating an existing isbsvc's persistence strategy (#1376)
 * [82538b6a6](https://github.com/numaproj/numaflow/commit/82538b6a65f280599ed59a2b0bcbfe0b7999ea1a) fix: add more checks to isbsvc validation (#1358)
 * [118c309d2](https://github.com/numaproj/numaflow/commit/118c309d27f84f9306933a4147a86f03cd9c65ef) fix: non-ack failed offsets (#1370)
 * [3456f7147](https://github.com/numaproj/numaflow/commit/3456f7147c8f512caad320e895f9ce0c23cbf741) feat: validate patched data for pipelines (#1349)
 * [e75e95811](https://github.com/numaproj/numaflow/commit/e75e958116550550f3a3bf8cadbdadcef1cc07f0) Add Atlan into USERS.md (#1351)
 * [483c01855](https://github.com/numaproj/numaflow/commit/483c01855be75a3dd903d351fe7a129c60f8b135) Unit tests UI (#1348)
 * [d0ae14843](https://github.com/numaproj/numaflow/commit/d0ae1484329e3d68fac1884e06dc39564c48367c) fix(SERVER): remove unknown filter (#1346)
 * [7ac77521e](https://github.com/numaproj/numaflow/commit/7ac77521e097fe897377a564da3a7062061976dc) Upstreammain (#1345)
 * [f1af1f02c](https://github.com/numaproj/numaflow/commit/f1af1f02c31ecc8950d61d9460bbd0d496ead01d) fix: rc-4 bug bash bug fixes (#1343)
 * [f6edd5ae5](https://github.com/numaproj/numaflow/commit/f6edd5ae53099ce174ab643ebd0608b447ddc399) fix(UI): rc-0.4 fixes (#1342)
 * [ecbd489f8](https://github.com/numaproj/numaflow/commit/ecbd489f87364caf50482f340cb2325d80f7960c) fix(SERVER): fix styles for ISB cards
 * [d4836e9e3](https://github.com/numaproj/numaflow/commit/d4836e9e3796c02e1e6ec30a6944febec12d1db1) fix(SERVER): fix styles for ISB cards
 * [953feb679](https://github.com/numaproj/numaflow/commit/953feb67991b1debceb8b69a85648434c49c882c) fix(SERVER): fix for pagination issue
 * [d484393d9](https://github.com/numaproj/numaflow/commit/d484393d92a489e61763978f1a1c754d5087fffb) fix(SERVER): namespace inout filtering space alignment
 * [e93601275](https://github.com/numaproj/numaflow/commit/e93601275981dc6129116f6086d104c4f78746ae) fix(SERVER): pipeline card style fix
 * [c24b91cea](https://github.com/numaproj/numaflow/commit/c24b91cead6dc094fca3764e6211860c3d9a668e) fix: block pipeline load post update (#1333)
 * [e928be34d](https://github.com/numaproj/numaflow/commit/e928be34de04d62b6ef6633b5593fd4efab3456f) fix: full isb spec in edit (#1331)
 * [d5c3b07a0](https://github.com/numaproj/numaflow/commit/d5c3b07a050b25a9a376e69e4be603fbdeb37dc8) RC2.0 UI fixes (#1329)
 * [f4211fb1e](https://github.com/numaproj/numaflow/commit/f4211fb1e1b40ad2bd47cc119e2bfdebbb747250) feat: container for generator vertices (#1321)
 * [f4354af30](https://github.com/numaproj/numaflow/commit/f4354af308b4de4e797609d45374b5a4bdd0d59e) fix: create isb should move to isb tab (#1323)
 * [24afc5e12](https://github.com/numaproj/numaflow/commit/24afc5e12a06c739f9ccd453e90a730f4049550b) fix: max lag (#1319)
 * [42f81df44](https://github.com/numaproj/numaflow/commit/42f81df44e4f80e24b72199b625e616acb06583e) feat: cache daemon client for each pipeline (#1276)
 * [a72704266](https://github.com/numaproj/numaflow/commit/a72704266b2634439e4929f8bb5dc54cf0262cde) fix: rc2 UI fixes (#1317)
 * [01bf18544](https://github.com/numaproj/numaflow/commit/01bf18544824e8b42291be64c4a16123ae54c752) feat: added filtering based on number, status and health of pipelines… (#1312)
 * [91b0effa1](https://github.com/numaproj/numaflow/commit/91b0effa1275d86c616ff3076bd35b75fe3a0660) fix: user identity cookie max age (#1316)
 * [5c999b631](https://github.com/numaproj/numaflow/commit/5c999b631f030402c2239daaf98bb68427747632) feat: add scopes to authorization (#1288)
 * [e119a0ee6](https://github.com/numaproj/numaflow/commit/e119a0ee666c8804c9296a3f61639ec181a86bf1) fix: logout fix (#1310)
 * [449dfd3a3](https://github.com/numaproj/numaflow/commit/449dfd3a3c085dea8d3ac3a6d1c8ba4a67c92fba) fix: Split cookie to meet the cookie length requirement (#1305)
 * [87c4c1e16](https://github.com/numaproj/numaflow/commit/87c4c1e16e5da8e583a14a97be25696652c72cb5) chore(deps): bump github.com/nats-io/nats-server/v2 from 2.10.3 to 2.10.4 (#1307)
 * [ea5774527](https://github.com/numaproj/numaflow/commit/ea5774527f9cd34868b379805eca0143acc0de7d) add tooltips (#1289)
 * [93eec9639](https://github.com/numaproj/numaflow/commit/93eec9639df4aa0f15fd7ce7e1588efc75a128b4) fix: fixed the timer not clearing issue (#1303)
 * [c7bdbda56](https://github.com/numaproj/numaflow/commit/c7bdbda569a264b618c468743af99ced9828780f) feat: k8s events filtering and cluster summary card fixes  (#1297)
 * [a558b229d](https://github.com/numaproj/numaflow/commit/a558b229d499ba6bffecefaa57c591428ef7c0b4) fix: graph overflow with large height (#1301)
 * [73ffaa86e](https://github.com/numaproj/numaflow/commit/73ffaa86e319da838fca12aa392f6b2ff171b9f2) doc: need metrics server (#1296)
 * [32416bf26](https://github.com/numaproj/numaflow/commit/32416bf263c4661086ae7d6f9046d2f31b137eaf) refactor: unified metrics names for forwarders (#1290)
 * [17b7b313f](https://github.com/numaproj/numaflow/commit/17b7b313fbed721c7ce3cb080c70e05a8d5675bd) feat: added separate colors for sideInput and dynamic legend (#1292)
 * [6efab64ef](https://github.com/numaproj/numaflow/commit/6efab64ef7f16936b470f39a43b7d67e6e188c20) feat: add tabs to display pipelines and isb services (#1293)
 * [86df4a840](https://github.com/numaproj/numaflow/commit/86df4a84083613cf3912f35ada400434ce1b200c) fix: more sidebar testing (#1287)
 * [d7ae1d36a](https://github.com/numaproj/numaflow/commit/d7ae1d36ad600b08d18632964ae5d8aea5c01504) refactor: create interfaces for AuthN and AuthZ (#1286)
 * [65aca23f6](https://github.com/numaproj/numaflow/commit/65aca23f6192a451033cfe25dff820dfb2102ae4) fix: tests for utils (#1283)

### Contributors

 * Bradley Behnke
 * Chandan Kumar
 * Darshan Simha
 * Derek Wang
 * Dillen Padhiar
 * Juanlu Yu
 * Keran Yang
 * Madusudanan.B.N
 * Shakira M
 * Sidhant Kohli
 * Vedant Gupta
 * Vigith Maurice
 * Yashash H L
 * dependabot[bot]
 * mshakira

## v1.0.0 (2023-11-03)

 * [78134e8f0](https://github.com/numaproj/numaflow/commit/78134e8f0396cb5d4acb7fe6d8cbcd2768f80f5a) Update manifests to v1.0.0
 * [660ff5010](https://github.com/numaproj/numaflow/commit/660ff5010b2191de4c015bccc7a4ad4be1e81388) fix: rc-4 bug bash bug fixes (#1343)
 * [ceed5def0](https://github.com/numaproj/numaflow/commit/ceed5def03c4d1d9c011b4839b2606a7741a39f3) fix(UI): rc-0.4 fixes (#1342)

### Contributors

 * Darshan Simha
 * Derek Wang
 * mshakira

## v1.0.0-rc4 (2023-11-03)

 * [e94b563f4](https://github.com/numaproj/numaflow/commit/e94b563f44a0469fc88bed0351d14e7587c0fb61) Update manifests to v1.0.0-rc4
 * [14757afcc](https://github.com/numaproj/numaflow/commit/14757afcc6d76603e8229357671d2092bab30f33) fix(SERVER): fix styles for ISB cards
 * [fc90920b9](https://github.com/numaproj/numaflow/commit/fc90920b93315830d24d0e16c5754823b9f7b3b7) fix(SERVER): fix styles for ISB cards
 * [8bf275bd3](https://github.com/numaproj/numaflow/commit/8bf275bd33c465276a1b714c4c2f5a7024f96c42) fix(SERVER): fix for pagination issue
 * [2e8cff7ac](https://github.com/numaproj/numaflow/commit/2e8cff7acb824ec76d07876651717c297407929e) fix(SERVER): namespace inout filtering space alignment
 * [c77fb0fcf](https://github.com/numaproj/numaflow/commit/c77fb0fcf66397642271b20bd86f9732110f8d19) fix(SERVER): pipeline card style fix
 * [52d9370f5](https://github.com/numaproj/numaflow/commit/52d9370f585f7984834dbd11c257c9fefb9b137f) fix: block pipeline load post update (#1333)
 * [2307660f7](https://github.com/numaproj/numaflow/commit/2307660f731a370929811dd584b3ad16383c3e1f) fix: full isb spec in edit (#1331)
 * [a70c77b93](https://github.com/numaproj/numaflow/commit/a70c77b93b4b8e7d5688a7cbabbba35392b1ea80) RC2.0 UI fixes (#1329)
 * [02281f38f](https://github.com/numaproj/numaflow/commit/02281f38fdbe84f6d28ad2414a47636135f093f5) feat: container for generator vertices (#1321)
 * [feb4977a9](https://github.com/numaproj/numaflow/commit/feb4977a9e806ec1dcf4b79e04f2991338df60c9) fix: create isb should move to isb tab (#1323)
 * [e05132ef9](https://github.com/numaproj/numaflow/commit/e05132ef9b2a9d9d2ffd500a9c8ddc5c5bee9b2f) fix: max lag (#1319)

### Contributors

 * Bradley Behnke
 * Darshan Simha
 * Derek Wang
 * Juanlu Yu
 * Shakira M
 * Vedant Gupta
 * mshakira

## v1.0.0-rc3 (2023-11-01)

 * [6ab96b182](https://github.com/numaproj/numaflow/commit/6ab96b1823a1de438cb1fb6a3733282fe4468fc1) Update manifests to v1.0.0-rc3
 * [4ffda3834](https://github.com/numaproj/numaflow/commit/4ffda3834fae7c2487cafd61855f6da520b07276) feat: cache daemon client for each pipeline (#1276)
 * [5a7d739e2](https://github.com/numaproj/numaflow/commit/5a7d739e233c0632aa8b36628687cd35f5a449d4) fix: rc2 UI fixes (#1317)
 * [cbd810bc4](https://github.com/numaproj/numaflow/commit/cbd810bc4b1835d55de0d4641313d44ec79ab2fe) feat: added filtering based on number, status and health of pipelines… (#1312)
 * [491c87867](https://github.com/numaproj/numaflow/commit/491c87867a05eaaac2e0f6cc4526aa490cfdc81b) fix: user identity cookie max age (#1316)

### Contributors

 * Darshan Simha
 * Derek Wang
 * Dillen Padhiar
 * Juanlu Yu
 * mshakira

## v1.0.0-rc2 (2023-11-01)

 * [8a7dc5920](https://github.com/numaproj/numaflow/commit/8a7dc5920c143aeaca06ff9c5cfc633102bac7d6) Update manifests to v1.0.0-rc2
 * [8ed52b39c](https://github.com/numaproj/numaflow/commit/8ed52b39c086532bc5b8e1a53352718219c718c6) feat: add scopes to authorization (#1288)
 * [eebe623f8](https://github.com/numaproj/numaflow/commit/eebe623f8464e37dc2e5674966917c142bb92441) fix: logout fix (#1310)
 * [9c34c27fb](https://github.com/numaproj/numaflow/commit/9c34c27fb1a16e78bdbf1be16ad321fd71f1801b) fix: Split cookie to meet the cookie length requirement (#1305)
 * [c6073b17c](https://github.com/numaproj/numaflow/commit/c6073b17c7e664095549774f6b578e205b86a1a4) chore(deps): bump github.com/nats-io/nats-server/v2 from 2.10.3 to 2.10.4 (#1307)
 * [b1bb65751](https://github.com/numaproj/numaflow/commit/b1bb65751aad08bbf3ff697f4f102ecbdbb09d92) add tooltips (#1289)
 * [40ca1705b](https://github.com/numaproj/numaflow/commit/40ca1705b6086c5683d67f8d5adc1805258c291e) fix: fixed the timer not clearing issue (#1303)
 * [2a91668d0](https://github.com/numaproj/numaflow/commit/2a91668d021c5346daab2188ba5353c84b3e9815) feat: k8s events filtering and cluster summary card fixes  (#1297)
 * [4929a1e45](https://github.com/numaproj/numaflow/commit/4929a1e4543549f8698d3d1510364353662d9816) fix: graph overflow with large height (#1301)
 * [8cc04a77a](https://github.com/numaproj/numaflow/commit/8cc04a77a0c12bc550f3783cb31274493094e996) doc: need metrics server (#1296)
 * [05b194005](https://github.com/numaproj/numaflow/commit/05b19400509cfeead2c96f52c6d33a243257c507) refactor: unified metrics names for forwarders (#1290)
 * [5475e6820](https://github.com/numaproj/numaflow/commit/5475e68205fe8899ae2f93bd73281e3e109e3e48) feat: added separate colors for sideInput and dynamic legend (#1292)
 * [052201e0b](https://github.com/numaproj/numaflow/commit/052201e0b803e0a57d562056d28d9b3b605728da) feat: add tabs to display pipelines and isb services (#1293)
 * [9e1ea4f07](https://github.com/numaproj/numaflow/commit/9e1ea4f07bbb1642f3d670c9c2398b58f262be06) fix: more sidebar testing (#1287)
 * [d018af50b](https://github.com/numaproj/numaflow/commit/d018af50bec64c1012a27d51fd407a548a364118) refactor: create interfaces for AuthN and AuthZ (#1286)
 * [ada5ea4d7](https://github.com/numaproj/numaflow/commit/ada5ea4d73c0ec11852328104d2cd386b288a81b) fix: tests for utils (#1283)

### Contributors

 * Bradley Behnke
 * Darshan Simha
 * Derek Wang
 * Juanlu Yu
 * Keran Yang
 * Sidhant Kohli
 * Vedant Gupta
 * Vigith Maurice
 * dependabot[bot]
 * mshakira

## v1.0.0-rc1 (2023-10-26)

 * [0ff1f58fa](https://github.com/numaproj/numaflow/commit/0ff1f58fa371795a321c414c94a40c6b08ca32fe) Update manifests to v1.0.0-rc1
 * [16c3fc3c5](https://github.com/numaproj/numaflow/commit/16c3fc3c55911688f0478b2d3966d0f6229d59e1) fix: incorrect image version for  namespaced numaflow-server (#1282)
 * [18d629358](https://github.com/numaproj/numaflow/commit/18d6293589a56ae7d7fc398871eec95564d0c41f) fix: ISBCreate test and fetch mock setup (#1279)
 * [9325140c8](https://github.com/numaproj/numaflow/commit/9325140c81e9dee50c407ab93fb1fd4510d95992) fix: update dex to work with basehref (#1278)
 * [36610a5ac](https://github.com/numaproj/numaflow/commit/36610a5ac107815460b1ba1dbb375d253925a372) feat: AuthN/AuthZ for Numaflow UI (#1234)
 * [730552e8d](https://github.com/numaproj/numaflow/commit/730552e8dc3acdcfc8607dc3f2875459ee9cc373) fix(doc): hpa api version (#1274)
 * [c103427b5](https://github.com/numaproj/numaflow/commit/c103427b553bda96b103e57bb1cd0e44e9a72cd2) fix: updating example.md (#1262)
 * [c1725b184](https://github.com/numaproj/numaflow/commit/c1725b1848a5fba88eead794e61dff125f49d06b) Feat/side input tests (#1257)
 * [5554bd657](https://github.com/numaproj/numaflow/commit/5554bd6572f62be9a0cff1fa74ab8d6896fcf1a6) chore(deps): bump google.golang.org/grpc from 1.57.0 to 1.57.1 (#1268)
 * [b86b22541](https://github.com/numaproj/numaflow/commit/b86b22541272474b8985be41aa0efb5826a5fce2) fix(UI): pod selection fix (#1266)
 * [07a7b8eac](https://github.com/numaproj/numaflow/commit/07a7b8eace4b08cf33696f35aafed3754cd257e5) doc: Numaflow high level security (#1264)
 * [308acf2f0](https://github.com/numaproj/numaflow/commit/308acf2f05df81e9c413025bcecb9645287e9546) fix: fixed the ns-summary page to allow creation of pipeline when no … (#1263)
 * [ce737732a](https://github.com/numaproj/numaflow/commit/ce737732a2b6f6748514555e4299b7efee8b7250) Unit tests graph page (#1250)
 * [639a93643](https://github.com/numaproj/numaflow/commit/639a936438e0ee8312d5c0534c5c3ddd46ecd24c) feat: Add e2e test for map sideinput, Fixes #1192 (#1211)
 * [7b32af34a](https://github.com/numaproj/numaflow/commit/7b32af34a7ab545bdc1b37b15f152538b83f5f1b) feat: get current status of ISB service (#1199)
 * [5c7fc90e6](https://github.com/numaproj/numaflow/commit/5c7fc90e633a8114cb681884ceae1e66c1d688b7) Summary view fixes (#1253)
 * [53aed68d4](https://github.com/numaproj/numaflow/commit/53aed68d49c0901230517504512e71745fb7fb2e) chore(deps): bump github.com/nats-io/nats-server/v2 from 2.9.19 to 2.9.23 (#1232)
 * [303829955](https://github.com/numaproj/numaflow/commit/303829955202784af8eee8bf37cc54d108f3b123) fix: updated div's with box and removed unwanted css (#1236)
 * [8b50f36ca](https://github.com/numaproj/numaflow/commit/8b50f36ca12b5c5045d8a32c6d4b62ef4219d6f4) chore(deps): bump @babel/traverse from 7.23.0 to 7.23.2 in /ui (#1221)
 * [7a3ca76a2](https://github.com/numaproj/numaflow/commit/7a3ca76a2ec7d3b0f06f7bc7e26e310b02685bb9) Update kafka.md (#1218)
 * [e49a18110](https://github.com/numaproj/numaflow/commit/e49a1811094824d3b004efb3905ce716956616ad) Update generator.md (#1217)
 * [9785eb070](https://github.com/numaproj/numaflow/commit/9785eb0707cad8d1384c78b58e12feb00f2ec48e) Update map.md (#1219)
 * [a874478a2](https://github.com/numaproj/numaflow/commit/a874478a2920b5a68b6d246c56a3d468b5dedc5e) feat: UI 1.0 CRUD (#1181)
 * [d34fcc47f](https://github.com/numaproj/numaflow/commit/d34fcc47fcfa94fc5ad2d16ca51a27d9c39748da) fix: get isbsvc kind apiversion (#1220)
 * [74f4d9809](https://github.com/numaproj/numaflow/commit/74f4d9809fde734cfca87db3675ec3d9d5e03ac7) Update kafka.md (#1215)
 * [7d5fe51ff](https://github.com/numaproj/numaflow/commit/7d5fe51ff7b1dfc6d95bb3688e03fa207d412ba2) Update generator.md (#1214)
 * [b6adac15c](https://github.com/numaproj/numaflow/commit/b6adac15cf62d82402f4bf6dff8fa6ee06f8b5a1) Update overview.md (#1213)
 * [0748449d9](https://github.com/numaproj/numaflow/commit/0748449d9d1ce094a23a557c8951f5e0e34d8f56) feat: add udsource python e2e (#1204)
 * [fea29657f](https://github.com/numaproj/numaflow/commit/fea29657f22ff89bccbf9f389ab983b1d901ddd9) doc: roadmap (#1208)
 * [d76305532](https://github.com/numaproj/numaflow/commit/d76305532bf5b920a13c508794f4ed61e30f6fb4) Namespace card status bar changes 0.11 (#1206)
 * [19a523aa8](https://github.com/numaproj/numaflow/commit/19a523aa89df2e44a43e77e258ebf24b75de56ea) feat: added unit tests for PipelineCard component (#1205)
 * [6a50f1301](https://github.com/numaproj/numaflow/commit/6a50f1301e4a76236aee58b73dd6b0cf7cc3b761) pods component error fix (#1203)
 * [a07e78f73](https://github.com/numaproj/numaflow/commit/a07e78f73032618296823a5bbea2dcf222122909) fix: updated image styles to a class (#1202)
 * [71c048ebe](https://github.com/numaproj/numaflow/commit/71c048ebe39d08389e88087673ce514f2f6c6bc6) feat: Changed the status bar component to an icon based component (#1198)
 * [13264e3fe](https://github.com/numaproj/numaflow/commit/13264e3fedd2bcb0c50b290db383943db50b64f3) fix(SERVER): restructure pod details component (#1189)
 * [1bbdcad88](https://github.com/numaproj/numaflow/commit/1bbdcad88ca9bc225ca8e38898c190e7cc5724d9) feat: updated the legend to a collapsible one on the top left (#1196)
 * [910243bd7](https://github.com/numaproj/numaflow/commit/910243bd7998cc088c5cad296ec4427f6c585f50) chore(deps): bump golang.org/x/net from 0.12.0 to 0.17.0 (#1190)
 * [57af20e5c](https://github.com/numaproj/numaflow/commit/57af20e5ce7291757c171629ccdee82b8532d384) feat: API Delete ISBSVC validation (#1182)
 * [2e1fd7017](https://github.com/numaproj/numaflow/commit/2e1fd70177daf8f2c6fa47b2bf5f00b7fca50897) added BCubed to the user list (#1184)
 * [b962f8e31](https://github.com/numaproj/numaflow/commit/b962f8e3164d2fd9bb8b3806f511decbe11a4244) feat: add timeout for pausing pipeline. Fixes #992 (#1138)
 * [ef62c5ca6](https://github.com/numaproj/numaflow/commit/ef62c5ca6a1127ee8d58b61737770185891f49cc) feat: Jetstream support for replica of 1 Fixes #944 (#1177)
 * [80294989b](https://github.com/numaproj/numaflow/commit/80294989be83478520f96d6f4ad911839cbb9f43) doc: minor clean up of JOIN doc (#1175)
 * [d4b5f1b26](https://github.com/numaproj/numaflow/commit/d4b5f1b2600c52264a3fb90504af8dc53740f99b) fix: incorrect side inputs watch logic (#1164)
 * [00e6b6ae6](https://github.com/numaproj/numaflow/commit/00e6b6ae691fda77f71c8932997dea994693576d) fix: not considered as back pressured when onFull is discardLatest (#1153)
 * [8b0d8cef9](https://github.com/numaproj/numaflow/commit/8b0d8cef902442cc7302f5dbe9577b00e085abaf) fix(SERVER): handle states when status is unknown (#1154)
 * [31b1aacdb](https://github.com/numaproj/numaflow/commit/31b1aacdb1c335cd7c69dccdb83e7c1fdc7c58cc) fix(SERVER): fix key warning (#1152)

### Contributors

 * Bradley Behnke
 * Caroline Dikibo
 * Chandan Kumar
 * Darshan Simha
 * Darshan Simha U
 * Dennis Sosa
 * Derek Wang
 * Dillen Padhiar
 * Joel Millage
 * Jorvaulx
 * Juanlu Yu
 * Kayla Nussbaum
 * Shubham Dixit
 * Sidhant Kohli
 * TASNEEM KOUSHAR
 * Vigith Maurice
 * aruwanip
 * bpcarey01
 * dependabot[bot]
 * mshakira

## v0.11.0 (2023-10-13)

 * [fbf51b2db](https://github.com/numaproj/numaflow/commit/fbf51b2db59f259d925471d40eede930441d9e71) Update manifests to v0.11.0
 * [f33d614fe](https://github.com/numaproj/numaflow/commit/f33d614fecc3affd1310415f01e36e2ff152b5ff) Namespace card status bar changes 0.11 (#1206)
 * [391b75dc7](https://github.com/numaproj/numaflow/commit/391b75dc7cca37ba9000f2e253f0256d2abeccb7) feat: added unit tests for PipelineCard component (#1205)
 * [7bcc1f278](https://github.com/numaproj/numaflow/commit/7bcc1f278de7f1815bb103543e06418d321f2c02) pods component error fix (#1203)
 * [df28f9379](https://github.com/numaproj/numaflow/commit/df28f937991a38ffb3daca5650ba166160b3dbc9) fix: updated image styles to a class (#1202)
 * [bbb7db77b](https://github.com/numaproj/numaflow/commit/bbb7db77b8ed79c3fec70f6ccd2f126b79d7d19c) feat: Changed the status bar component to an icon based component (#1198)
 * [f6f20d6c8](https://github.com/numaproj/numaflow/commit/f6f20d6c89c024e616b0d21406f08a5e5002c302) fix(SERVER): restructure pod details component (#1189)
 * [96a6002ee](https://github.com/numaproj/numaflow/commit/96a6002eedb6d7570815f0c02b1666a912291f47) feat: updated the legend to a collapsible one on the top left (#1196)
 * [575605b42](https://github.com/numaproj/numaflow/commit/575605b42adc7e34227894aabe64635e4030b0e7) chore(deps): bump golang.org/x/net from 0.12.0 to 0.17.0 (#1190)
 * [bfec4fc56](https://github.com/numaproj/numaflow/commit/bfec4fc56d79948af025d28ce8657ab11a0c1ed6) added BCubed to the user list (#1184)
 * [1e0b25fd0](https://github.com/numaproj/numaflow/commit/1e0b25fd0b7556af3333002c149d53c14602b50a) feat: add timeout for pausing pipeline. Fixes #992 (#1138)
 * [ae2327646](https://github.com/numaproj/numaflow/commit/ae23276463f55553ef2604d7eba55d796d994671) feat: Jetstream support for replica of 1 Fixes #944 (#1177)
 * [1e405b37e](https://github.com/numaproj/numaflow/commit/1e405b37e92e2e898cc757f5d4ab9b67c5b7fef0) doc: minor clean up of JOIN doc (#1175)
 * [77c01811c](https://github.com/numaproj/numaflow/commit/77c01811c5ab9df188a0559f51151e66141d9088) fix: incorrect side inputs watch logic (#1164)

### Contributors

 * Darshan Simha U
 * Derek Wang
 * Dillen Padhiar
 * Joel Millage
 * Vigith Maurice
 * dependabot[bot]
 * mshakira

## v0.11.0-rc2 (2023-10-03)

 * [8ae28af3f](https://github.com/numaproj/numaflow/commit/8ae28af3fed4ed201f1eb40d3556377a04c9681f) Update manifests to v0.11.0-rc2
 * [f74bde398](https://github.com/numaproj/numaflow/commit/f74bde398d2e35e84c7752952597898956b9dc69) fix: not considered as back pressured when onFull is discardLatest (#1153)
 * [689aa2954](https://github.com/numaproj/numaflow/commit/689aa2954ad279e7c91184dbf2f3690cde806fb2) fix(SERVER): handle states when status is unknown (#1154)
 * [f83ea00a8](https://github.com/numaproj/numaflow/commit/f83ea00a8e93c51c2f914ca383a91b5b2f05984e) fix(SERVER): fix key warning (#1152)

### Contributors

 * Derek Wang
 * mshakira

## v0.11.0-rc1 (2023-10-03)

 * [1d89a12bc](https://github.com/numaproj/numaflow/commit/1d89a12bc331e7daafaa30db540c5ac57ca50257) Update manifests to v0.11.0-rc1
 * [882bcef3b](https://github.com/numaproj/numaflow/commit/882bcef3b3c2fbd37558f58bf5f424ead4be1aaa) feat: Numaflow UI 1.0 (#1077)
 * [a7adee1ea](https://github.com/numaproj/numaflow/commit/a7adee1eabc66c530d295ded25b27005055d1537) fix: treat ALL user-defined source vertices as scalable (#1132)
 * [408ff389c](https://github.com/numaproj/numaflow/commit/408ff389c56358a4ef6f98bf0f89cf1fe9bd415c) feat: add doc link checker (#1130)
 * [dae11a7fb](https://github.com/numaproj/numaflow/commit/dae11a7fb8ad0b1650877bc864dc4c7c9506c996) extracting redis streams source (#1113)
 * [9d65e229e](https://github.com/numaproj/numaflow/commit/9d65e229e0f8b92154dbbd2d50e707bf19f205aa) refactor: shared kubeconfig util (#1095)
 * [bb6f29b23](https://github.com/numaproj/numaflow/commit/bb6f29b23c8eea8b9db8b3293d04df5baaac9fff) fix: wrong api group in webhook rbac settings (#1086)
 * [4150eb289](https://github.com/numaproj/numaflow/commit/4150eb289355bf22a994a02a272b30fe4e93f50d) feat: set correct number of replices for some types of vertices after resuming pipeline (#1085)
 * [2182b83b7](https://github.com/numaproj/numaflow/commit/2182b83b7f089821b3e780a61aef851bab34faa2) chore(deps): bump graphql from 16.6.0 to 16.8.1 in /ui (#1078)
 * [b2a377caf](https://github.com/numaproj/numaflow/commit/b2a377caf34e906f338cc3bff7d804d58195ba0b) feat: add forest validation for pipelines. Fixes #1002 (#1063)
 * [9035ba8b4](https://github.com/numaproj/numaflow/commit/9035ba8b44677aa8805d17926520e3ce5a0d0ffe) feat: implement pipeline validation for unsupported states (#1043)
 * [4369d65f5](https://github.com/numaproj/numaflow/commit/4369d65f5071591b033720ef941467650dac6afc) fix: message count read in forwarder (#1030)
 * [8d52d78bd](https://github.com/numaproj/numaflow/commit/8d52d78bda240e96dabbbb9575f4c127d001d34d) fix(ci): Wrong test pod image tag used for running CI on release branch (#1041)
 * [6ed2c381e](https://github.com/numaproj/numaflow/commit/6ed2c381e4b1d089e8442fee86eac331e30d3e83) fix: allow udsource pending api to return negative count to indicate PendingNotAvailable (#1040)
 * [c12e90941](https://github.com/numaproj/numaflow/commit/c12e9094131c033e3ab98d79b3d289577080ad88) refactor: re-arrange some of the rater implementations (#1036)
 * [abe332e6d](https://github.com/numaproj/numaflow/commit/abe332e6d4cc09f3540440de17d9ec6225cf3c21) fix(docs): file names and links for side inputs (#1037)
 * [20fb7cc29](https://github.com/numaproj/numaflow/commit/20fb7cc29c1eae2fb148fd70b173c3c61beb7674) doc: Add side input docs (#1029)
 * [b373406cc](https://github.com/numaproj/numaflow/commit/b373406ccdb4436f0bd6f568ebc3eb2f4f341fb9) doc: kill-switch when buffer is full (#1034)
 * [9f5127f04](https://github.com/numaproj/numaflow/commit/9f5127f04ceb95fe130387da192c834b5b1c6ffd) fix: Idle handler refactor (#1021)
 * [b1f8b0269](https://github.com/numaproj/numaflow/commit/b1f8b0269669b58d90cc4982dd8bf28ee30c8fb0) fix: calculate processing rate for sink vertices (#1025)
 * [c01a14068](https://github.com/numaproj/numaflow/commit/c01a1406825131d5a6adfe9214d55fbab232a24b) feat: colored logs for UI with toggle for logs order (#1022)

### Contributors

 * Derek Wang
 * Dillen Padhiar
 * Juanlu Yu
 * Julie Vogelman
 * Keran Yang
 * Sidhant Kohli
 * Tianchu Zhao
 * Vedant Gupta
 * Vigith Maurice
 * dependabot[bot]

## v0.10.1 (2023-09-14)

 * [e5e2b6191](https://github.com/numaproj/numaflow/commit/e5e2b6191a386a2e6fdb546e2d57b5ee69fb18ef) Update manifests to v0.10.1
 * [4702849c2](https://github.com/numaproj/numaflow/commit/4702849c298b17f908b7de47d96ce94699661cc7) feat: implement pipeline validation for unsupported states (#1043)
 * [e36b3c6ce](https://github.com/numaproj/numaflow/commit/e36b3c6ceb08e1c9a48cccce484643dadea630ff) fix: message count read in forwarder (#1030)
 * [01d9abee0](https://github.com/numaproj/numaflow/commit/01d9abee0ba1faf2636f7dff4b5601be19c35bb6) fix: allow udsource pending api to return negative count to indicate PendingNotAvailable (#1040)
 * [8cbb4d673](https://github.com/numaproj/numaflow/commit/8cbb4d67331bc0ef8db5d52157ec7e2270b02711) refactor: re-arrange some of the rater implementations (#1036)
 * [b511effc2](https://github.com/numaproj/numaflow/commit/b511effc2a85608ee3b372b0cb36ee77385afb7e) fix(docs): file names and links for side inputs (#1037)
 * [5b083c2d2](https://github.com/numaproj/numaflow/commit/5b083c2d29fbb4740d3aadfc8622cadadfbe1d00) doc: Add side input docs (#1029)
 * [8d5c56f89](https://github.com/numaproj/numaflow/commit/8d5c56f8968eadc04c87cc0db308bc05ac418d87) doc: kill-switch when buffer is full (#1034)
 * [73db23a3f](https://github.com/numaproj/numaflow/commit/73db23a3f834de99e9c9456d978692a3a8f55c71) fix: Idle handler refactor (#1021)
 * [7119ed965](https://github.com/numaproj/numaflow/commit/7119ed965723e3b55d1ac8484dcb7bc2e091c6bf) fix: calculate processing rate for sink vertices (#1025)
 * [6408137da](https://github.com/numaproj/numaflow/commit/6408137dad05d24690394a1143548da2bc618ccb) feat: colored logs for UI with toggle for logs order (#1022)
 * [2311260cc](https://github.com/numaproj/numaflow/commit/2311260cce0aebf32552a1c55c890f7e5e606c6d) fix(ci): Wrong test pod image tag used for running CI on release branch (#1041)

### Contributors

 * Derek Wang
 * Dillen Padhiar
 * Juanlu Yu
 * Keran Yang
 * Sidhant Kohli
 * Vedant Gupta
 * Vigith Maurice

## v0.10.0 (2023-09-05)

 * [10d1cfde2](https://github.com/numaproj/numaflow/commit/10d1cfde26f168e19dabc797485fc766d117f086) Update manifests to v0.10.0
 * [6f7c1f4a9](https://github.com/numaproj/numaflow/commit/6f7c1f4a950a808a8e3f575a3223b1a0d5d44aaa) fix: seg fault inside controller (#1016)
 * [c2fdef163](https://github.com/numaproj/numaflow/commit/c2fdef16338e99b6cc26778705a7789937d7b49b) fix: reconcile headless services before pods (#1014)
 * [7d8b90874](https://github.com/numaproj/numaflow/commit/7d8b90874b37fcf91d4bf04432ece2816a04d513) fix: print version info when starting (#1013)
 * [247b89ed9](https://github.com/numaproj/numaflow/commit/247b89ed9aa5e17142b982f955c01bfe2a6650ed) feat: join vertex UI support (#1010)
 * [aabb8af0a](https://github.com/numaproj/numaflow/commit/aabb8af0a9dee4338c2ece6c35eaee4f4a169a20) feat: scaleUpCooldownSeconds and scaleDownCooldownSeconds to replace cooldownSeconds (#1008)
 * [ad647ab75](https://github.com/numaproj/numaflow/commit/ad647ab7549efac87160ca0a0f69a34153aaa887) chore(deps): bump @adobe/css-tools from 4.2.0 to 4.3.1 in /ui (#1005)
 * [92fbf7f15](https://github.com/numaproj/numaflow/commit/92fbf7f15dfd296dcbc20214753fb43550a9d1fd) fix: avoid unwanted watcher creation and reduce being stuck with udf is restarted (#999)
 * [bac06df00](https://github.com/numaproj/numaflow/commit/bac06df00748f2f1b84be210e2f79c6f280ebb9b) fix: missing edges on UI (#998)
 * [f90d4fe7b](https://github.com/numaproj/numaflow/commit/f90d4fe7bff838cfed3001920965f33c57105f3d) feat: Add side input sdkclient and grpc  (#953)
 * [d99480a89](https://github.com/numaproj/numaflow/commit/d99480a890d00be99c4d2fc33f2856eae38db4d0) feat: implement user-defined source (#980)
 * [706859025](https://github.com/numaproj/numaflow/commit/706859025e2baaa1c77b77d9b83ec6bcf32129d0) fix: send keys for udsink (#979)
 * [8f32b9a3e](https://github.com/numaproj/numaflow/commit/8f32b9a3e4ac4a55fb275f0407dc67f1f3523b4a) fix bulleted list (#977)
 * [1f33bf8b4](https://github.com/numaproj/numaflow/commit/1f33bf8b45039ce235b930047ab3b77e0f1d8635) refactor: build wmstore and wmstorewatcher directly, and remove some unnecessary fields  (#970)
 * [4cea3444a](https://github.com/numaproj/numaflow/commit/4cea3444ae1a375d2550ccd7b66e0541fece169c) feat: add vertex template to pipeline spec (#947)
 * [4a4ed9275](https://github.com/numaproj/numaflow/commit/4a4ed9275c6da00588df72434f6e082e0bb0dd99) feat: Add side-input initializer and synchronizer (#912)
 * [d10f36e67](https://github.com/numaproj/numaflow/commit/d10f36e67581c76516554fd60acd400de45c2607) fix: npe when the ctx is canceled inside kv watcher (#942)
 * [6b1b3337c](https://github.com/numaproj/numaflow/commit/6b1b3337c76bfdbe2a53173f97ce43ee993577ad) fix: retry logic for fetching last updated kv time (#939)
 * [e3da4a3ef](https://github.com/numaproj/numaflow/commit/e3da4a3ef31191f61d75ebeab8ba5f76cfba0e17) fix: close the watermark fetcher and publishers after all the forwarders exit (#921)
 * [2d6112bf0](https://github.com/numaproj/numaflow/commit/2d6112bf0c20baace28da76e6fb0ace3c3be01b5) Pipelines with Cycles: e2e testing, and pipeline validation (#920)
 * [5e0bf77e6](https://github.com/numaproj/numaflow/commit/5e0bf77e6fbbb73ff267271064d7443dcdffac86) docs quick fixes (#919)
 * [0f8f7a17b](https://github.com/numaproj/numaflow/commit/0f8f7a17b0a1e1259e771dafed38c71db3443543) docs updates (#917)
 * [b55566b89](https://github.com/numaproj/numaflow/commit/b55566b89b568b4211a467e98cb48a0c4b7ea884) feat: watermark delay in tooltip (#910)
 * [667ada751](https://github.com/numaproj/numaflow/commit/667ada75146ee4594ef6603fa06fb1c93e141a89) fix: removing WIP tag (#914)
 * [872aa8640](https://github.com/numaproj/numaflow/commit/872aa8640c08c776b7cea9da4afa09a1a9098cc3) feat: emit k8s events for controller messages. Fixes #856 (#901)
 * [0fbdb7aba](https://github.com/numaproj/numaflow/commit/0fbdb7aba3fc6e15f6b81146fdf7a6acdae08868) fix: avoid potential deadlocks when operating UniqueStringList (#905)
 * [2c85ec439](https://github.com/numaproj/numaflow/commit/2c85ec439098a313f4c33829a0ef8d9db30a0ea0) refactor: avoid exposing internal data structures of pod tracker to the rater (#902)
 * [7e86306bc](https://github.com/numaproj/numaflow/commit/7e86306bcb1c0cc66f08172d668ad5f14c7ca503) feat: Join Vertex (#875)
 * [85360f652](https://github.com/numaproj/numaflow/commit/85360f6528139721fff37048eccd0e605fc53418) fix: stabilize nats connection (#889)
 * [d4f8f5943](https://github.com/numaproj/numaflow/commit/d4f8f59431e1322ce6a555018efd821801e69a12) doc: Update multi partition doc (#898)
 * [404672d68](https://github.com/numaproj/numaflow/commit/404672d68ed0777e94ee98ef008461bc5687d101) fix: Reduce idle WM unit test fix (#897)
 * [a1bbdedf4](https://github.com/numaproj/numaflow/commit/a1bbdedf44289efc46773dd1d004b586d8663037) updated default version of Redis used for e2e (#891)
 * [85ee4b0d8](https://github.com/numaproj/numaflow/commit/85ee4b0d8669f4b24fe202e6b9e7389e85826912) fix TestBuiltinEventTimeExtractor (#885)
 * [f3e1044ea](https://github.com/numaproj/numaflow/commit/f3e1044eab7e2372dcdf2779d74e4ce8cb5f7cfb) chore(deps): bump word-wrap from 1.2.3 to 1.2.4 in /ui (#881)
 * [a02f29a71](https://github.com/numaproj/numaflow/commit/a02f29a710fd482b093193df11387e287d2c7c2e) fix: remove retry when the processor is not found. (#868)
 * [cfdeaa8a4](https://github.com/numaproj/numaflow/commit/cfdeaa8a4b50ffae8edcfd3a940ba194e354f0b6) refactor: create a new data forwarder dedicated for source (#874)
 * [6d14998a9](https://github.com/numaproj/numaflow/commit/6d14998a998e392590a8871da87514f5bffa6a46) feat: controller changes for Side Inputs support (#866)
 * [92db62a90](https://github.com/numaproj/numaflow/commit/92db62a907cb410a74ef20e066e5ed8aea10bf78) fix: highlight edge when buffer is full (#869)
 * [9c4e83c06](https://github.com/numaproj/numaflow/commit/9c4e83c0658dd319cf43eced249af965a9e8af18) fix: minor ui bugs (#861)
 * [b970b4cc7](https://github.com/numaproj/numaflow/commit/b970b4cc7dfe90dd01a086a333e654275d5aeb7f) fix: release script for validating webhook (#860)
 * [7684aada2](https://github.com/numaproj/numaflow/commit/7684aada2fb57458c572a088fbfc5c9ffc1a07e5) fix: use windower to fetch next window yet to be closed (#850)
 * [609d8b3ce](https://github.com/numaproj/numaflow/commit/609d8b3ce2233b18f9c6debfc4c8ec65ee067dfa) feat: implement optional validation webhook. Fixes #817. (#832)
 * [3ae1cedb3](https://github.com/numaproj/numaflow/commit/3ae1cedb3918d9936b0f2098b853f1d96d5e6e60) chore(deps): bump semver from 6.3.0 to 6.3.1 in /ui (#845)

### Contributors

 * Derek Wang
 * Dillen Padhiar
 * Jason Zesheng Chen
 * Juanlu Yu
 * Julie Vogelman
 * Keran Yang
 * RohanAshar
 * Sidhant Kohli
 * Vedant Gupta
 * Vigith Maurice
 * Yashash H L
 * dependabot[bot]

## v0.9.3 (2023-09-05)

 * [6141719f3](https://github.com/numaproj/numaflow/commit/6141719f327e8f8d5b5176c75cd41b179622de96) Update manifests to v0.9.3
 * [022f8bfae](https://github.com/numaproj/numaflow/commit/022f8bfae8aa7e59abf0c795410f17312586e502) fix: seg fault inside controller (#1016)

### Contributors

 * Derek Wang
 * Yashash H L

## v0.9.2 (2023-08-23)

 * [6f81361f8](https://github.com/numaproj/numaflow/commit/6f81361f8f36fad74208895aab599e63f9436b79) Update manifests to v0.9.2
 * [66c32197d](https://github.com/numaproj/numaflow/commit/66c32197db97ed4335aacb0267558bc83026e788) fix: error when kv_watch with no keys (#981)

### Contributors

 * Derek Wang

## v0.9.1 (2023-08-11)

 * [4cbd729c9](https://github.com/numaproj/numaflow/commit/4cbd729c91e0387781d00d97bfd8e0b61d0fd8c7) Update manifests to v0.9.1
 * [aa5e8ae3e](https://github.com/numaproj/numaflow/commit/aa5e8ae3ef064fa7140480347c40c74c73b01f25) fix: npe when the ctx is canceled inside kv watcher (#942)
 * [e5a5cd6c0](https://github.com/numaproj/numaflow/commit/e5a5cd6c0859c25cdf1110d4804ec4f88b0b068c) feat: watermark delay in tooltip (#910)

### Contributors

 * Derek Wang
 * Vedant Gupta
 * Yashash H L

## v0.9.0 (2023-08-02)

 * [8e4b6ca13](https://github.com/numaproj/numaflow/commit/8e4b6ca1337ff444b348be597e036728fca9d757) Update manifests to v0.9.0
 * [7424ae509](https://github.com/numaproj/numaflow/commit/7424ae50910c399227dfc4b1eae2d31c295513cc) feat: emit k8s events for controller messages. Fixes #856 (#901)
 * [d0bfac6d5](https://github.com/numaproj/numaflow/commit/d0bfac6d5e130e6f9e1f97c873ad3ca404c3c2fd) fix: avoid potential deadlocks when operating UniqueStringList (#905)
 * [75c7f975b](https://github.com/numaproj/numaflow/commit/75c7f975b13505ad1b9abe674a736db23d2021d5) fix: stabilize nats connection (#889)
 * [0db1238db](https://github.com/numaproj/numaflow/commit/0db1238db3f33684604fea3fa5b367e5d4f3a3c3) fix: Reduce idle WM unit test fix (#897)
 * [5073f1c81](https://github.com/numaproj/numaflow/commit/5073f1c81946d58a3757e4c658299ccdc9534d77) fix TestBuiltinEventTimeExtractor (#885)
 * [33b7d1d08](https://github.com/numaproj/numaflow/commit/33b7d1d08387903f59bcce2cf182879a4de54c79) fix: remove retry when the processor is not found. (#868)
 * [89b2d1c4d](https://github.com/numaproj/numaflow/commit/89b2d1c4d0cd0e996469b5e8acb389845b626f53) fix: highlight edge when buffer is full (#869)
 * [8d49c0f67](https://github.com/numaproj/numaflow/commit/8d49c0f67a2cf905a48748525bb02dd50ab1bedc) fix: minor ui bugs (#861)
 * [9478e3026](https://github.com/numaproj/numaflow/commit/9478e3026c71c2108166b2f94070819e7e232bba) fix: release script for validating webhook (#860)

### Contributors

 * Derek Wang
 * Dillen Padhiar
 * Juanlu Yu
 * Keran Yang
 * Vedant Gupta
 * Yashash H L

## v0.9.0-rc2 (2023-07-13)

 * [d0df669a8](https://github.com/numaproj/numaflow/commit/d0df669a8bb9f07fffe1d5add792444ebfb33835) Update manifests to v0.9.0-rc2
 * [c8aaeff8c](https://github.com/numaproj/numaflow/commit/c8aaeff8ca796b43279d9b784883d632bf4b8d32) fix: use windower to fetch next window yet to be closed (#850)
 * [bcda8dcfc](https://github.com/numaproj/numaflow/commit/bcda8dcfcdbde7821c201b37f5d5ce99148a341c) feat: implement optional validation webhook. Fixes #817. (#832)
 * [e605504d4](https://github.com/numaproj/numaflow/commit/e605504d40fce533c64569d0d30a8533df62299d) chore(deps): bump semver from 6.3.0 to 6.3.1 in /ui (#845)

### Contributors

 * Derek Wang
 * Dillen Padhiar
 * Yashash H L
 * dependabot[bot]

## v0.9.0-rc1 (2023-07-11)

 * [40f454107](https://github.com/numaproj/numaflow/commit/40f45410780c51c8109650e2835a105543d3f77c) Update manifests to v0.9.0-rc1
 * [f5276dbb2](https://github.com/numaproj/numaflow/commit/f5276dbb227a8ff3b0a21b257790a7eb2f282911) fix: pod tracker logic for calculating processing rate (#838)
 * [db06e7e4b](https://github.com/numaproj/numaflow/commit/db06e7e4b36e608fc19e578e71adc7d96c2d8197) chore(deps): bump tough-cookie from 4.1.2 to 4.1.3 in /ui (#839)
 * [b660b6d99](https://github.com/numaproj/numaflow/commit/b660b6d99118c945a6a466f3387fbade4a1984e0) fix: resource leak inside daemon server (#837)
 * [1f19a7421](https://github.com/numaproj/numaflow/commit/1f19a7421d6194aa25ed9fa2d3a2327ba6bbf449) feat: capability to increase max message size (#835)
 * [c61ce319f](https://github.com/numaproj/numaflow/commit/c61ce319f133aad18e6cf3dd597fde11b991b22f) doc: update roadmap (#830)
 * [aca1c9bf4](https://github.com/numaproj/numaflow/commit/aca1c9bf4495ad986c46bfd0cd58f2556cea9599) feat: add stragglers (late data) into the window is window is open (#824)
 * [0155b4a53](https://github.com/numaproj/numaflow/commit/0155b4a539e95d9254d9bc6bd57a29436f01ea12)  fix(docs): fixed some incorrect docs and renamed a timeExtractionFilter arg (#814)
 * [dd060cb80](https://github.com/numaproj/numaflow/commit/dd060cb80637ad5904d3000785c0214a80894e7e) feat: rater changes to track processing rate per partition (#805)
 * [541ceb20f](https://github.com/numaproj/numaflow/commit/541ceb20f0105567f220b101efe775ee3a027a08) fix: metric to track watermark bug was wrongly tagged (#809)
 * [cf4731519](https://github.com/numaproj/numaflow/commit/cf473151974b1d907be88addb40d9bb549a0bbd0) feat: autoscaling changes to support multi partition (#806)
 * [6a5ee1a5b](https://github.com/numaproj/numaflow/commit/6a5ee1a5b8a3b237a3299a094f03c4ae79fa77de) fix: segmentation fault in daemon server (#804)
 * [2ce0ac90c](https://github.com/numaproj/numaflow/commit/2ce0ac90ce8f274b78b6d78a8b9d9138d6e0777e) fix: Intermittent failure from Kafka to get consumer offsets (#803)
 * [32be7fc54](https://github.com/numaproj/numaflow/commit/32be7fc54de762c30fea5343f1f0a982c7b5f626) feat: support UI for multipartition edges (#789)
 * [97db1984a](https://github.com/numaproj/numaflow/commit/97db1984ac5f14cd53e4d09b4ceb827f45b23f2e) refactor: remove redundant delta calculations for rater (#795)
 * [3406a1304](https://github.com/numaproj/numaflow/commit/3406a13044308b81a7e9f1d462d05f99e059bbb3) fix: select pods not in evicted status (#786)
 * [a9204fbc8](https://github.com/numaproj/numaflow/commit/a9204fbc8b069357ded8f7a2ea9f8d7f7b767854) feat: combine built-in UDTransformers for filter and eventTime assignment (#783)
 * [85955e306](https://github.com/numaproj/numaflow/commit/85955e306d899992ce6118069f804f02f2f28bef) feat: support multi-partitioned edges (#751)
 * [5ce6936d1](https://github.com/numaproj/numaflow/commit/5ce6936d161d45e5050a038e1eaaf54b229a4eb0) fix: duplicate ui served from gin Router (#781)
 * [e9ea7d855](https://github.com/numaproj/numaflow/commit/e9ea7d855682d91e1a0d30d4c8694bf875018d54) fix: unexpected high processing rates (#780)
 * [f60b8ab93](https://github.com/numaproj/numaflow/commit/f60b8ab936b76020523b22a28dc54450f76d75d1) chore(deps): bump github.com/gin-gonic/gin from 1.9.0 to 1.9.1 (#772)
 * [466e38048](https://github.com/numaproj/numaflow/commit/466e380484f08b6f74f8dfd4f8c47a9ab4196599) feat: gRPC error handling (#744)
 * [9aff2bd88](https://github.com/numaproj/numaflow/commit/9aff2bd88bc2eaebf61d6898571f2ca601c3afe7) feat: forwardAChunk to support multi partitioned edges (#757)
 * [f32731709](https://github.com/numaproj/numaflow/commit/f32731709c24cf26df9ce523f9e026cd0fbac51a) fix: pipeline view fix (#755)
 * [037c9a612](https://github.com/numaproj/numaflow/commit/037c9a612b6d138014f6d83ab56fdb92c89227de) fix: toVertexPartitions for reduce was incorrectly populated to 1 (#756)
 * [099b914ac](https://github.com/numaproj/numaflow/commit/099b914ac776625d3e64d1db3e05be1543376587) feat: use metrics to calculate vertex processing rate (#743)
 * [59880e97c](https://github.com/numaproj/numaflow/commit/59880e97c2ae850e00f280ddb7551d58197e48e4) feat: enable streaming message to next vertex when batch size is 1 (#709)
 * [160b94140](https://github.com/numaproj/numaflow/commit/160b9414085138c67b0c421523a29844a6559b63) fix: use int32 for message length (#750)
 * [e383ee2f6](https://github.com/numaproj/numaflow/commit/e383ee2f6d09146c6ccbf5c76ea9627e63b779fa) feat: using one bucket for partitioned reduce watermark propagation (#742)
 * [ba1f493dc](https://github.com/numaproj/numaflow/commit/ba1f493dc57acd2ba080a8107adbaa6b7c27fa7a) fix(test): flakey test (#738)
 * [f0c832910](https://github.com/numaproj/numaflow/commit/f0c8329101bddd6f4f7e06d95ce6c283c45311c6) refactor: buffer, edge, bucket (#733)
 * [eb9a7c4cd](https://github.com/numaproj/numaflow/commit/eb9a7c4cdfb7b4dd2e5f30190587b5a184fe2c06) feat: change baseHref for Numaflow UI. Fixes #375. (#698)
 * [b1f639e7e](https://github.com/numaproj/numaflow/commit/b1f639e7e48da7f6ebd98bf164a12ed8335f2be6) fix: let kafka source crash and restart when there is any server side error (#735)
 * [c8aafc7f7](https://github.com/numaproj/numaflow/commit/c8aafc7f7da7008c6c83fae8d70e49bc045a2f0e) feat: Autoscale for Redis Streams Source (#726)
 * [d8074ec16](https://github.com/numaproj/numaflow/commit/d8074ec16ffeaccb6e60b5eab0ccb6898cf385f9) feat: Redis7 as an ISB svc (#717)
 * [3dd2a31db](https://github.com/numaproj/numaflow/commit/3dd2a31db9835d9052929f11ff42323cdbf865fc) chore(deps): bump github.com/gin-gonic/gin from 1.8.1 to 1.9.0 (#724)
 * [15a229b51](https://github.com/numaproj/numaflow/commit/15a229b5191820e96f6d0f251dfc33f704ea7cd7) docs(proposal): edges, buffers and buckets (#704)
 * [714c80360](https://github.com/numaproj/numaflow/commit/714c80360f9a77aa060c058b1361e37fd682b629) chore(doc): update README with demo (#718)
 * [431778def](https://github.com/numaproj/numaflow/commit/431778def1f15c2cd7b3b119f609662ef7464212) doc: add overview (#713)
 * [f518d99c8](https://github.com/numaproj/numaflow/commit/f518d99c8bb8561594b3099f3da19f30661e50e2) feat: allowedLateness to support late data ingestion (#703)
 * [1efac4264](https://github.com/numaproj/numaflow/commit/1efac4264c185ad2fa806f69f02a48cfe902d58a) fix: allow late message as long as window is not closed (#696)
 * [3da84fa51](https://github.com/numaproj/numaflow/commit/3da84fa519a10cee0dddc37a31bd8ed4e3219482) fix: add wal dir x permission (#689)
 * [3a91cc52f](https://github.com/numaproj/numaflow/commit/3a91cc52f507d310ad8a926e13ad30e691e05df3) chore(doc): refactor doc struct (#685)

### Contributors

 * Derek Wang
 * Dillen Padhiar
 * Juanlu Yu
 * Julie Vogelman
 * Keran Yang
 * Vedant Gupta
 * Vigith Maurice
 * Yashash H L
 * dependabot[bot]
 * xdevxy

## v0.8.1 (2023-05-30)

 * [4b1193870](https://github.com/numaproj/numaflow/commit/4b11938700d8dadc8d3e4ba47a7a04e11659c3bd) Update manifests to v0.8.1
 * [67277b794](https://github.com/numaproj/numaflow/commit/67277b794766dadc1a77bc77baa70277fdacf07c) fix: pipeline view fix (#755)
 * [7cb399e96](https://github.com/numaproj/numaflow/commit/7cb399e96676e43af942baf9ef2c20165c16e41f) fix: toVertexPartitions for reduce was incorrectly populated to 1 (#756)
 * [16067af2b](https://github.com/numaproj/numaflow/commit/16067af2b057df5f7a09f2de90f93cadc86a651e) feat: use metrics to calculate vertex processing rate (#743)
 * [11cd8e9f2](https://github.com/numaproj/numaflow/commit/11cd8e9f2bf960cfe2516e900071e4e552c53ee2) feat: enable streaming message to next vertex when batch size is 1 (#709)
 * [a50588407](https://github.com/numaproj/numaflow/commit/a5058840735f9d78c5ca9ae408c66375290dbfa9) fix: use int32 for message length (#750)
 * [c602a5203](https://github.com/numaproj/numaflow/commit/c602a5203d397585030362a3db9b9ee60dfac572) feat: using one bucket for partitioned reduce watermark propagation (#742)
 * [ccf79c6d5](https://github.com/numaproj/numaflow/commit/ccf79c6d548aa5043bdc2a11de79cc861da250d6) fix(test): flakey test (#738)
 * [af8e3346e](https://github.com/numaproj/numaflow/commit/af8e3346e08d3076c71f6bb649c7af8c2574943a) refactor: buffer, edge, bucket (#733)
 * [d57bfed42](https://github.com/numaproj/numaflow/commit/d57bfed42a457264087190d54662916ec9fc1589) feat: change baseHref for Numaflow UI. Fixes #375. (#698)
 * [37dfae58a](https://github.com/numaproj/numaflow/commit/37dfae58a8719f5fe806e442c0701b8f2dc5e11c) fix: let kafka source crash and restart when there is any server side error (#735)
 * [b7a0dda51](https://github.com/numaproj/numaflow/commit/b7a0dda51b185b7d0e819bbb70a97511552b2c49) feat: Autoscale for Redis Streams Source (#726)
 * [5654e0aff](https://github.com/numaproj/numaflow/commit/5654e0affe3910ee0fa7497f684fa3e1ab6babe2) feat: Redis7 as an ISB svc (#717)
 * [9e8d8cc42](https://github.com/numaproj/numaflow/commit/9e8d8cc42428cc5b8d84d9668a19a70b341a8914) chore(deps): bump github.com/gin-gonic/gin from 1.8.1 to 1.9.0 (#724)

### Contributors

 * Derek Wang
 * Dillen Padhiar
 * Julie Vogelman
 * Keran Yang
 * Vedant Gupta
 * Yashash H L
 * dependabot[bot]
 * xdevxy

## v0.8.0 (2023-04-26)

 * [e57ca739d](https://github.com/numaproj/numaflow/commit/e57ca739d3ca7539f2090c2509580174269b0e44) Update manifests to v0.8.0
 * [652be8d60](https://github.com/numaproj/numaflow/commit/652be8d60c13c6b43f9aabe0f317876dcbdcda23) feat: allowedLateness to support late data ingestion (#703)
 * [8e7e3b61e](https://github.com/numaproj/numaflow/commit/8e7e3b61efaf2c1b695d4c6e2c14a4c631b8baee) fix: allow late message as long as window is not closed (#696)
 * [fcaed47d0](https://github.com/numaproj/numaflow/commit/fcaed47d0a4654376bed2e500274dee370e7f848) fix: add wal dir x permission (#689)
 * [aae08fa1b](https://github.com/numaproj/numaflow/commit/aae08fa1b93cb156af1b977608e7bf73dabcd42f) chore(doc): refactor doc struct (#685)

### Contributors

 * Derek Wang
 * Vigith Maurice

## v0.8.0-rc1 (2023-04-14)

 * [ca88313d5](https://github.com/numaproj/numaflow/commit/ca88313d52c67700b7d7d74a6e326235783a06f6) Update manifests to v0.8.0-rc1
 * [b83525dfa](https://github.com/numaproj/numaflow/commit/b83525dfa60cabea55797ad6a0d27f0e43f26d88) feat: introducing tags for conditional forwarding (#668)
 * [a6e81746b](https://github.com/numaproj/numaflow/commit/a6e81746b43787a42a4a0e78808ea99aef5bfc9a) feat: expose cpu/mem info to sidecar containers (#678)
 * [c7b853aad](https://github.com/numaproj/numaflow/commit/c7b853aad2cc4189e5666ee4e21b9c60913f3ad0) feat: Redis Streams source fixes (#669)
 * [2f73b5b84](https://github.com/numaproj/numaflow/commit/2f73b5b84b7ed771cbd18f76c9455aa86003b8d3) fix: skip empty Kafka partitions when calculating pending count (#666)
 * [eeb37d8b6](https://github.com/numaproj/numaflow/commit/eeb37d8b6842e29e17491cde327cad503d06f143) feat: support for multi keys (#658)
 * [91b516b8a](https://github.com/numaproj/numaflow/commit/91b516b8ad6ad3940f8721613a7d12c982ce6ed2) feat: Adds SASL (plain and gssapi) support for kafka sink (#656)
 * [196f887d3](https://github.com/numaproj/numaflow/commit/196f887d31378a6ad6868bb1eb5e837512c31b04) fix: vertex overlapping watermark (#660)
 * [0db3248d0](https://github.com/numaproj/numaflow/commit/0db3248d0e0618f2d4b6fac9266414681e2bedb0) feat: incremental search and namespace preview in search bar (#654)
 * [46bb87505](https://github.com/numaproj/numaflow/commit/46bb875053f82b0b8ee21cbcd91d55d72bef9d17) feat: integrate serde WAL (#650)
 * [b05608762](https://github.com/numaproj/numaflow/commit/b056087626034f6775f8da8def906a4e9d0131c5) fix: unit test (#653)
 * [1b0ea0881](https://github.com/numaproj/numaflow/commit/1b0ea088134b464252f3c2296556bd97fa4acb9b) feat: handle idle watermark for reduce vertex (#627)
 * [33882628a](https://github.com/numaproj/numaflow/commit/33882628adadb2f8f36c5519c41c50398ffd86eb) feat: Redis streams source (#628)
 * [d85bf93fd](https://github.com/numaproj/numaflow/commit/d85bf93fd5f99ce37d4e0c36cc27ae417e938fec) feat: Adds SASL (plain and gssapi) support for kafka source (#643)
 * [60bb2bb96](https://github.com/numaproj/numaflow/commit/60bb2bb96a66b495a8243433f9e31c87ee303c9e) feat: namespace scope api and disable namespace search on UI (#638)
 * [38b5a9ec8](https://github.com/numaproj/numaflow/commit/38b5a9ec8115f4bb77f34a793fcbaf3e701e0b19) fix: GetHeadWatermark Logic (#636)
 * [927b95cd0](https://github.com/numaproj/numaflow/commit/927b95cd007b71a1494cf11cc9614048e5b45898) feat: enable edge-level kill switch to drop messages when buffer is full, for the non-reduce forwarder (#634)
 * [0c79113c5](https://github.com/numaproj/numaflow/commit/0c79113c53619e30bc51b7687f9cdc313f752679) fix: IdleWatermark unit test (#640)
 * [924ad33eb](https://github.com/numaproj/numaflow/commit/924ad33eb516eda4295d7af276bff130216e3e0a) fix: desired replicas should not be greater than pending (#639)
 * [150c5c23e](https://github.com/numaproj/numaflow/commit/150c5c23e6898be41937d8f0331bef7742ce7baa) fix: add timeout to the test (#618)
 * [2f112fb29](https://github.com/numaproj/numaflow/commit/2f112fb29ad4723937f95d73a93b16526f6aec20) feat: kustomize integration (#637)
 * [5062aac60](https://github.com/numaproj/numaflow/commit/5062aac603421c4d95a94133852643104c9f3337) fix: exclude ack pending messages (#631)
 * [e533ba357](https://github.com/numaproj/numaflow/commit/e533ba3572dffaf58e807000b00fe84b48b4539e) feat: UI error component (#613)
 * [20aaca9de](https://github.com/numaproj/numaflow/commit/20aaca9de98db83a43ebb59773c425cd269759d1) fix: do not update status.replicas until pod operation succeeds (#620)
 * [cc62c81c7](https://github.com/numaproj/numaflow/commit/cc62c81c72b4d5025af35a1467c59cb3c77804a4) feat: track and expose oldest work yet to be done to the reduce loop (#617)
 * [4bbe80bb3](https://github.com/numaproj/numaflow/commit/4bbe80bb39deb4b723a4f6e239581a7791c5cb9f) feat: handle watermark barrier for map vertex (#607)
 * [f9f05442f](https://github.com/numaproj/numaflow/commit/f9f05442fdf5f2c498066b45e2e10dd36193fb10) fix: corrected reduce vertex replica number. Fixes #593 (#616)
 * [c9815132b](https://github.com/numaproj/numaflow/commit/c9815132b1f747be09af7c7e005f93c050ecdfca) feat: add API for pipeline status check. Fixes #407. (#599)
 * [5282766d0](https://github.com/numaproj/numaflow/commit/5282766d074e57fd5581650f372858e5c8b7519c) chore(deps): bump webpack from 5.74.0 to 5.76.1 in /ui (#610)
 * [927bfc047](https://github.com/numaproj/numaflow/commit/927bfc047033098480f0f861fabcee3d60b60daa) feat: use randomized shuffle using vertex name as the seed (#601)
 * [d667d799f](https://github.com/numaproj/numaflow/commit/d667d799fc1e56eb46d33fa4daf4dfb5cf5367a0) fix: ack the dropped messages as well (#603)
 * [64e17d884](https://github.com/numaproj/numaflow/commit/64e17d884a325f04171afe642c17aff3818c79ab) feat: enable controller HA (#602)
 * [20ba722f6](https://github.com/numaproj/numaflow/commit/20ba722f6c478d5d07a84f45ff9869216e0a246b) feat: expose dnspolicy and dnsconfig to pod template (#598)
 * [abfdd78fe](https://github.com/numaproj/numaflow/commit/abfdd78fe138721f6311c0260a348706f68469f2) Chore: tickgen changes to test reduce pipelines (#587)
 * [b2f8a12a4](https://github.com/numaproj/numaflow/commit/b2f8a12a49405a8a29e530fc50f01b410e0ef3de) feat: bidirectional streaming (#553)
 * [148663e75](https://github.com/numaproj/numaflow/commit/148663e7583af02dcb665b922d687e215ab7a5df) feat: use customized binary serde for nats message payload (#585)
 * [8d339b684](https://github.com/numaproj/numaflow/commit/8d339b68421e8490425a2168b969aec01823949b) fix: Idle watermark fix for read batch size > 0 and partial idle outgoing edges (#575)
 * [87ab1e3d3](https://github.com/numaproj/numaflow/commit/87ab1e3d34d4e550fe6973225036ce891066fc5e) feat: implement watermark propagation for source data transformer (#557)
 * [d561867f6](https://github.com/numaproj/numaflow/commit/d561867f69d1bac96eb26624e830c7193424d091) feat: namespace search (#559)
 * [1b4800afe](https://github.com/numaproj/numaflow/commit/1b4800afe96797cea557ab15ccd9ee0af03965d3) fix: refine log for buffer validation. Fixes #185 (#573)
 * [4eb27effe](https://github.com/numaproj/numaflow/commit/4eb27effe0fbcfc0f369b5dbeb185aab182fcc87) feat: add readiness and liveness check for daemon server. Fixes #543 (#571)
 * [8ae2116d2](https://github.com/numaproj/numaflow/commit/8ae2116d280fb1faf30525d2eeb1f1c72fd84986) feat: marshal/unmarshal binary for read message (#565)
 * [a0505e67f](https://github.com/numaproj/numaflow/commit/a0505e67f71db63cfc9bbbd4b83d27f773cdae54) chore(deps): bump golang.org/x/net from 0.0.0-20220722155237-a158d28d115b to 0.7.0 (#568)
 * [fbf36894e](https://github.com/numaproj/numaflow/commit/fbf36894e26f0f832077dfc0baf35b88fbe579e5) chore(deps): bump golang.org/x/text from 0.3.7 to 0.3.8 (#567)
 * [92c8009de](https://github.com/numaproj/numaflow/commit/92c8009dedd760beb3a01331e3db05a726157718) feat: expose image pull policy to user defined containers (#563)
 * [88a41c2b0](https://github.com/numaproj/numaflow/commit/88a41c2b0404975d875b12f87c4e856b7168defc) fix: typos in reduce examples (#556)
 * [97567f3cc](https://github.com/numaproj/numaflow/commit/97567f3cc7901845ea59d4a76741e13df6989048) feat: edge-watermark (#537)
 * [1e06ba2f7](https://github.com/numaproj/numaflow/commit/1e06ba2f708c6061f36f8e870fcde257a2014984) feat: enable envFrom for user defined containers (#554)
 * [0dc85f694](https://github.com/numaproj/numaflow/commit/0dc85f694e6f1048d4616705b3fa85fdb5bda9cd) feat: remove secret watch privilege dependency (#542)
 * [8b7e397e5](https://github.com/numaproj/numaflow/commit/8b7e397e5ffbb28f70f01b20446c909ced7a4f25) fix: Use a copied object to update (#541)
 * [943e7bd83](https://github.com/numaproj/numaflow/commit/943e7bd83dbdee38af28c4ee1c445ba31564d53f) chore(deps): bump github.com/emicklei/go-restful from 2.9.5+incompatible to 2.16.0+incompatible (#539)
 * [93753c152](https://github.com/numaproj/numaflow/commit/93753c1527104f86748444a321663d52b15dea98) feat: improve reduce performance (#501)
 * [b502fa93c](https://github.com/numaproj/numaflow/commit/b502fa93c5f2a59aa38c8ed7d3fa626aff39436c) feat: Offset time idle watermark put (#529)
 * [4551505c5](https://github.com/numaproj/numaflow/commit/4551505c5ca59b56018827aa657995766cd24e94) fix: securityContext not applied to container templates (#528)
 * [2727e62ac](https://github.com/numaproj/numaflow/commit/2727e62ac3271c7178bd6bc797c68979b1617538) feat: idle watermark v0 (#520)
 * [1e34e3154](https://github.com/numaproj/numaflow/commit/1e34e31542816f99ba377e6d85121ac6a6df9b4a) feat: Reduce UI Support (#500)
 * [39ecae427](https://github.com/numaproj/numaflow/commit/39ecae427080800cb062bd65713b16b37ff596bf) feat: enable RuntimeClassName for vertex pod (#519)
 * [b965318d2](https://github.com/numaproj/numaflow/commit/b965318d21e264682b868bff9268d7ecf54a8c92) feat: add builtin filter and event time extractor for source transformer (#517)
 * [077771cef](https://github.com/numaproj/numaflow/commit/077771cef79e798241f2cda5e01385834f451ee6) chore(deps): bump ua-parser-js from 0.7.32 to 0.7.33 in /ui (#507)
 * [58b12ec3e](https://github.com/numaproj/numaflow/commit/58b12ec3eb996045428df215011da1d88a3aa8a5) Add an e2e test for source data transformer (#505)
 * [2af91933e](https://github.com/numaproj/numaflow/commit/2af91933ee9b5dfc3feb1dfaebe81364c22a949d) feat: Implement source data transformer and apply to all existing sources (#487)
 * [a3024f4e2](https://github.com/numaproj/numaflow/commit/a3024f4e26c5a3467c3b526f7083ab57948a8678) fix: -ve metrics and return early if isLate (#495)
 * [229861537](https://github.com/numaproj/numaflow/commit/2298615374ff699179384627cfe6e5b13da793af) fix: JetStream context KV store/watch fix (#460)
 * [2177d6213](https://github.com/numaproj/numaflow/commit/2177d6213184d4a0eebc69981c1d363848d09b42) doc: reduce persistent store (#458)
 * [3c6212072](https://github.com/numaproj/numaflow/commit/3c62120725aa940831b901881a2d17192bcb0ee8) doc: reduce documentation (#448)
 * [784fe15cb](https://github.com/numaproj/numaflow/commit/784fe15cba31ccdfc986bf7bd8a526ef409805b0) chore(deps): bump json5 from 1.0.1 to 1.0.2 in /ui (#454)
 * [659a98b5e](https://github.com/numaproj/numaflow/commit/659a98b5e8ffcbc3fc62775f8ce21df8f3ff2f6b) refactor: simplify http request construction in test cases (#444)
 * [cc9c194bf](https://github.com/numaproj/numaflow/commit/cc9c194bf05671071f6416dea4d0b0d92f67617f) refactor: use exact matching instead of regex to perform e2e data validation. (#443)
 * [f7f712b34](https://github.com/numaproj/numaflow/commit/f7f712b34a640103cec327cf862ff2a11ef6a4c7) doc: windowing fixed and sliding (#439)
 * [9ad504b57](https://github.com/numaproj/numaflow/commit/9ad504b5791ddd22516aabe721fb94dd252dc289) refactor: move redis sink resources creation to E2ESuite (#437)
 * [0148258da](https://github.com/numaproj/numaflow/commit/0148258daa7b87fca65f04f70b8d769bb7796468) refactor: a prototype for enhancing E2E test framework (#424)
 * [8579dc675](https://github.com/numaproj/numaflow/commit/8579dc67516ec6c85f61c0b3e473d02c688296ff) feat: pipeline watermark (#416)

### Contributors

 * Alex Ang HH
 * Derek Wang
 * Dillen Padhiar
 * Juanlu Yu
 * Julie Vogelman
 * Keran Yang
 * Vedant Gupta
 * Vigith Maurice
 * Yashash H L
 * ashwinidulams
 * dependabot[bot]

## v0.7.3 (2023-04-02)

 * [68a14793f](https://github.com/numaproj/numaflow/commit/68a14793ff698f68d45e404e0269de32fd3c8ed3) Update manifests to v0.7.3
 * [a17a41df7](https://github.com/numaproj/numaflow/commit/a17a41df7324c7f72b4dcd09592593143c57fc76) feat: integrate serde WAL (#650)
 * [096c6acf6](https://github.com/numaproj/numaflow/commit/096c6acf6b82cf540e387c731b96ddad8e9861a2) fix: unit test (#653)
 * [cce50ffd1](https://github.com/numaproj/numaflow/commit/cce50ffd16a39931fe031603e53256ffe593ccae) feat: handle idle watermark for reduce vertex (#627)
 * [968cc5f50](https://github.com/numaproj/numaflow/commit/968cc5f50acec697a482c2bcdd93935c71d202c2) feat: Redis streams source (#628)
 * [38baae8aa](https://github.com/numaproj/numaflow/commit/38baae8aa1b4a7152b2aa9d537b6934fc1cb2f11) feat: Adds SASL (plain and gssapi) support for kafka source (#643)
 * [563b85b19](https://github.com/numaproj/numaflow/commit/563b85b19a5ec9f3c58ad86d6cdee1bab4849d12) feat: namespace scope api and disable namespace search on UI (#638)
 * [c8194690f](https://github.com/numaproj/numaflow/commit/c8194690f383624fd0e27c75b87069266fb6590d) fix: GetHeadWatermark Logic (#636)
 * [d37f4db5c](https://github.com/numaproj/numaflow/commit/d37f4db5c29155bf084ca8b84a642020041c92af) feat: enable edge-level kill switch to drop messages when buffer is full, for the non-reduce forwarder (#634)
 * [a0dce69a3](https://github.com/numaproj/numaflow/commit/a0dce69a35c11a54a1c399e77a91bacb1a646882) fix: IdleWatermark unit test (#640)
 * [f840e1c30](https://github.com/numaproj/numaflow/commit/f840e1c30a94d8d0b0644304b2bec71bbb565869) fix: desired replicas should not be greater than pending (#639)
 * [0ca5630e6](https://github.com/numaproj/numaflow/commit/0ca5630e6695fe9d526b8a1a000e44211a7610a2) fix: add timeout to the test (#618)
 * [b049f0b4e](https://github.com/numaproj/numaflow/commit/b049f0b4e37fd9ff37bfffb18ab39baf5acf0d09) feat: kustomize integration (#637)
 * [01305ea3e](https://github.com/numaproj/numaflow/commit/01305ea3ec8e34fdd92a239bcf4ca2f63cc00032) fix: exclude ack pending messages (#631)
 * [4ec4b3d7f](https://github.com/numaproj/numaflow/commit/4ec4b3d7f450ed8f7d5da5838223f560750dde03) feat: UI error component (#613)
 * [90ca505bb](https://github.com/numaproj/numaflow/commit/90ca505bb027ec9d1ae79c728d6af2a86110edae) fix: do not update status.replicas until pod operation succeeds (#620)
 * [339db43b6](https://github.com/numaproj/numaflow/commit/339db43b65136a2339d77b12edca166e78d47845) feat: track and expose oldest work yet to be done to the reduce loop (#617)
 * [1ce4c383c](https://github.com/numaproj/numaflow/commit/1ce4c383caf29384e937f6686e52f6561f8d93d2) feat: handle watermark barrier for map vertex (#607)
 * [90dbe1fab](https://github.com/numaproj/numaflow/commit/90dbe1fab20ad1421706bba772ae9352d16e674b) fix: corrected reduce vertex replica number. Fixes #593 (#616)
 * [a155f2af0](https://github.com/numaproj/numaflow/commit/a155f2af09522b8fb87a14af3559e1a2a74ddb57) feat: add API for pipeline status check. Fixes #407. (#599)
 * [d9e3a56f6](https://github.com/numaproj/numaflow/commit/d9e3a56f6ed25f4baf05f22ab058ac393a3ab4a3) chore(deps): bump webpack from 5.74.0 to 5.76.1 in /ui (#610)

### Contributors

 * Alex Ang HH
 * Derek Wang
 * Dillen Padhiar
 * Juanlu Yu
 * Julie Vogelman
 * Keran Yang
 * Vedant Gupta
 * Vigith Maurice
 * Yashash H L
 * dependabot[bot]

## v0.7.2 (2023-03-13)

 * [1196a24b7](https://github.com/numaproj/numaflow/commit/1196a24b7096273b60c072e803a336c79eef2c5b) Update manifests to v0.7.2
 * [a6f64f8d9](https://github.com/numaproj/numaflow/commit/a6f64f8d93f20bfa91b592fd00333068633bc2c2) feat: use randomized shuffle using vertex name as the seed (#601)
 * [16b20a357](https://github.com/numaproj/numaflow/commit/16b20a357ade0c0bae9edb0461ee01340025eb7f) fix: ack the dropped messages as well (#603)
 * [4cdef1749](https://github.com/numaproj/numaflow/commit/4cdef17494cf72a8d1081945c5bd23c68ffe70cd) feat: enable controller HA (#602)
 * [d2e0513c5](https://github.com/numaproj/numaflow/commit/d2e0513c5bb3358649da42b1babff4ee46c9e8a6) feat: expose dnspolicy and dnsconfig to pod template (#598)
 * [6002c829f](https://github.com/numaproj/numaflow/commit/6002c829f4600ef67d72fdda0e4693364dccfc07) Chore: tickgen changes to test reduce pipelines (#587)
 * [b1aee9453](https://github.com/numaproj/numaflow/commit/b1aee94533594cbee6470257d21a3d13c840ce70) feat: bidirectional streaming (#553)
 * [67fc688ab](https://github.com/numaproj/numaflow/commit/67fc688ab8be984d48182d3a0f4b9b9b99a8c2c8) feat: use customized binary serde for nats message payload (#585)
 * [cc1041995](https://github.com/numaproj/numaflow/commit/cc1041995d13ed899b3fce8fca955541cebf4588) fix: Idle watermark fix for read batch size > 0 and partial idle outgoing edges (#575)
 * [df1574dad](https://github.com/numaproj/numaflow/commit/df1574dad468cbc68eadb75b7a61c930d6f31578) feat: implement watermark propagation for source data transformer (#557)
 * [45d5c396d](https://github.com/numaproj/numaflow/commit/45d5c396d2f369f0a9724704d91a7e07aa64895f) feat: namespace search (#559)
 * [b14d470fa](https://github.com/numaproj/numaflow/commit/b14d470fa2b8bcd746f7e75b4ec20453297d53e6) fix: refine log for buffer validation. Fixes #185 (#573)
 * [a8e8bb158](https://github.com/numaproj/numaflow/commit/a8e8bb1580dcd80e2a3625a2dc6c0a7c95a823ef) feat: add readiness and liveness check for daemon server. Fixes #543 (#571)
 * [fd6acb6da](https://github.com/numaproj/numaflow/commit/fd6acb6da5b63c743225df3ea3743935f211aaba) feat: marshal/unmarshal binary for read message (#565)
 * [d1032b4ce](https://github.com/numaproj/numaflow/commit/d1032b4ce08a509223b5a7dbce4570b54d9e90a5) chore(deps): bump golang.org/x/net from 0.0.0-20220722155237-a158d28d115b to 0.7.0 (#568)
 * [fd00ebdf9](https://github.com/numaproj/numaflow/commit/fd00ebdf938933bc2b735e03a7df53bedf1f48d7) chore(deps): bump golang.org/x/text from 0.3.7 to 0.3.8 (#567)
 * [05ec77f5d](https://github.com/numaproj/numaflow/commit/05ec77f5d839dd13d6c1ef1cded94178f01d45e6) feat: expose image pull policy to user defined containers (#563)
 * [ecbe3a006](https://github.com/numaproj/numaflow/commit/ecbe3a0061e02b05077f8a29649b692802a986a5) fix: typos in reduce examples (#556)
 * [0dc3f5c6d](https://github.com/numaproj/numaflow/commit/0dc3f5c6d6e99c96937c5ee3a46f3f7e2723363f) feat: edge-watermark (#537)
 * [77298c853](https://github.com/numaproj/numaflow/commit/77298c853e0184f554bf22b13858cba0e35ed922) feat: enable envFrom for user defined containers (#554)

### Contributors

 * Derek Wang
 * Dillen Padhiar
 * Juanlu Yu
 * Julie Vogelman
 * Keran Yang
 * Vedant Gupta
 * Vigith Maurice
 * Yashash H L
 * ashwinidulams
 * dependabot[bot]

## v0.7.1 (2023-02-14)

 * [92925c154](https://github.com/numaproj/numaflow/commit/92925c15485a802600c5cb54cf603f2c1ceae027) Update manifests to v0.7.1
 * [2f8e147a3](https://github.com/numaproj/numaflow/commit/2f8e147a3106bead03655584a41e9951c7c17950) feat: remove secret watch privilege dependency (#542)
 * [f8e7daae4](https://github.com/numaproj/numaflow/commit/f8e7daae4f11b8c6a96676dd11b95d13efa830b2) fix: Use a copied object to update (#541)
 * [98de2459b](https://github.com/numaproj/numaflow/commit/98de2459b9a2ed733fa2a0c3be804a7f8241156f) chore(deps): bump github.com/emicklei/go-restful from 2.9.5+incompatible to 2.16.0+incompatible (#539)
 * [0df812c1c](https://github.com/numaproj/numaflow/commit/0df812c1c950f76a825847ea4b9c61d836102c38) feat: improve reduce performance (#501)
 * [ab49de684](https://github.com/numaproj/numaflow/commit/ab49de684aacbf5876da956dbad62d92b1ffa6ac) feat: Offset time idle watermark put (#529)
 * [c0aa7c1e3](https://github.com/numaproj/numaflow/commit/c0aa7c1e3769a8a9180e0a4b1a0f84facf393046) fix: securityContext not applied to container templates (#528)
 * [ac33fb026](https://github.com/numaproj/numaflow/commit/ac33fb0266f7ddc8be04f466c75c370f2a4e90cc) feat: idle watermark v0 (#520)
 * [2844cfb60](https://github.com/numaproj/numaflow/commit/2844cfb60480a9711f6e5ea4f233cf3fa37e9e9b) feat: Reduce UI Support (#500)
 * [e701180df](https://github.com/numaproj/numaflow/commit/e701180df3c218894a65e2b75de2f3811c21dd40) feat: enable RuntimeClassName for vertex pod (#519)
 * [bb94f6318](https://github.com/numaproj/numaflow/commit/bb94f6318e32d9e0ae1cf4fd494ea06334cf1a03) feat: add builtin filter and event time extractor for source transformer (#517)
 * [4562196d0](https://github.com/numaproj/numaflow/commit/4562196d00bf4db7d1c3d43bbcfcd4d699f864f1) chore(deps): bump ua-parser-js from 0.7.32 to 0.7.33 in /ui (#507)
 * [764cefdaa](https://github.com/numaproj/numaflow/commit/764cefdaa52465aa936283a5c6574c4757a79f78) Add an e2e test for source data transformer (#505)
 * [7665d6cef](https://github.com/numaproj/numaflow/commit/7665d6cef3cc8330bfe5b826d6bbfbd57e240568) feat: Implement source data transformer and apply to all existing sources (#487)
 * [d0226084b](https://github.com/numaproj/numaflow/commit/d0226084b020d35f618669cb56481badd07e8f38) fix: -ve metrics and return early if isLate (#495)

### Contributors

 * Derek Wang
 * Juanlu Yu
 * Keran Yang
 * Vedant Gupta
 * Vigith Maurice
 * ashwinidulams
 * dependabot[bot]

## v0.7.0 (2023-01-13)

 * [734e5d3b4](https://github.com/numaproj/numaflow/commit/734e5d3b44dee2ef690c9a1fe4d9d1ecb092a16c) Update manifests to v0.7.0
 * [5d6c53369](https://github.com/numaproj/numaflow/commit/5d6c53369a6ea8da5f9f5036fce9aa81f6308fbf) fix: JetStream context KV store/watch fix (#460)
 * [d6152e772](https://github.com/numaproj/numaflow/commit/d6152e772a03e646f2841a34497d498e4c2234c3) doc: reduce persistent store (#458)
 * [ac77656de](https://github.com/numaproj/numaflow/commit/ac77656dec8164fc162b8339bfff8138d17f73b0) doc: reduce documentation (#448)
 * [257356af0](https://github.com/numaproj/numaflow/commit/257356af0c932a9c7e84573c9f163e37a3e06dc4) chore(deps): bump json5 from 1.0.1 to 1.0.2 in /ui (#454)
 * [7752db4b7](https://github.com/numaproj/numaflow/commit/7752db4b7d4233e2a691c0d1cc9ef2348dc75ab5) refactor: simplify http request construction in test cases (#444)
 * [1a10af4c2](https://github.com/numaproj/numaflow/commit/1a10af4c20f051e46c063f9d946a39c208b6ec60) refactor: use exact matching instead of regex to perform e2e data validation. (#443)
 * [2777e27ac](https://github.com/numaproj/numaflow/commit/2777e27ac0cdfcc954bf5e453b92b7f4e8a5c201) doc: windowing fixed and sliding (#439)
 * [70fc008ff](https://github.com/numaproj/numaflow/commit/70fc008ffb0016a7310612d7cac191920207d0a6) refactor: move redis sink resources creation to E2ESuite (#437)
 * [6c078b420](https://github.com/numaproj/numaflow/commit/6c078b42046b4733f702b3fbb585578d6304dafb) refactor: a prototype for enhancing E2E test framework (#424)
 * [e7021c9ae](https://github.com/numaproj/numaflow/commit/e7021c9ae668724c11ac81fb49527ae8ce0f9240) feat: pipeline watermark (#416)

### Contributors

 * Derek Wang
 * Juanlu Yu
 * Keran Yang
 * Vedant Gupta
 * Vigith Maurice
 * dependabot[bot]

## v0.7.0-rc1 (2022-12-16)

 * [71887db5c](https://github.com/numaproj/numaflow/commit/71887db5cce231b9b0a236f8f00ddeb0d40ac01a) Update manifests to v0.7.0-rc1
 * [dda4835d8](https://github.com/numaproj/numaflow/commit/dda4835d87993dffba16b5e8a2a9e4b6b0e6cdba) feat: reduce metrics. Closes #313 (#414)
 * [85dbe4d7f](https://github.com/numaproj/numaflow/commit/85dbe4d7f43433ad2a17531f053dc91ee829835c) feat: udsink grpc stream (#421)
 * [fa07587f3](https://github.com/numaproj/numaflow/commit/fa07587f3a032a49e73827c4f069480add8eceb9) chore(doc): scope UDF under a dir (#426)
 * [0a911da92](https://github.com/numaproj/numaflow/commit/0a911da927c1cb61943430fad8edef2f3b1f661b) feat: sliding window. closes #339 (#354)
 * [a46fb9647](https://github.com/numaproj/numaflow/commit/a46fb9647c990ab69b0071bfc0ea6dfd6019f1bf) refactor: nats/jetstream testing (#418)
 * [13d95c487](https://github.com/numaproj/numaflow/commit/13d95c487b8ea2ef8b0897557745e4a9c825ee1d) feat: nats as source (#411)
 * [f1e7c7373](https://github.com/numaproj/numaflow/commit/f1e7c73732f064596f2543559b8101262f72f61d) fix: adding lock while discovering partitions, Closes #412 (#413)
 * [3b64d674f](https://github.com/numaproj/numaflow/commit/3b64d674f049907bcfb9b9558e45cca80f21f915) fix(test): e2e-api-pod can not start on M1 mac (#410)
 * [6504a5620](https://github.com/numaproj/numaflow/commit/6504a5620481208c214048f89f4f01f918f5586c) fix: getWatermark to return-1 if any processor returns -1  (#402)
 * [d4d22041d](https://github.com/numaproj/numaflow/commit/d4d22041d5438d31e5106c708974d2bfebff8e96) fix: e2e testing for PBQ WAL with reduce pipeline (#393)
 * [80e978503](https://github.com/numaproj/numaflow/commit/80e978503d3b5d2db36c231da8f2fda5cd4ccc8e) feat: add Grafana instruction and a dashboard template. Closes #287 (#381)
 * [2f94a915b](https://github.com/numaproj/numaflow/commit/2f94a915be7627a6ef3349f5e47b30f47dd63561) fix: unit tests for replay. Closes #373 (#377)
 * [8f367ab2e](https://github.com/numaproj/numaflow/commit/8f367ab2e0a02b7d5c8c7654bf16a34023dadc96) chore(docs): update docs (#380)
 * [efe4d41cc](https://github.com/numaproj/numaflow/commit/efe4d41ccd56dea30753c477b42004301ed3209a) fix: best effort processing during SIGTERM. Closes #371 (#372)
 * [7e041d873](https://github.com/numaproj/numaflow/commit/7e041d87308493bc21e1c317ed9e79c6ada2b725) feat(wal): First pass to implement WAL and hook to PBQ store. (#344)
 * [256e66b32](https://github.com/numaproj/numaflow/commit/256e66b326ae5a0c9959758f18ad9bc07c40fd65) feat: watermark otwatcher enhancement (#364)
 * [f8170577e](https://github.com/numaproj/numaflow/commit/f8170577eada6972e64f5d338b9d387f23111e47) refactor(docs): group docs in categories (#362)
 * [1a5d424f8](https://github.com/numaproj/numaflow/commit/1a5d424f8a1d93a681d5897eb1bcceee4e851bb2) chore(deps): bump loader-utils from 2.0.3 to 2.0.4 in /ui (#356)
 * [6c8f03f28](https://github.com/numaproj/numaflow/commit/6c8f03f28024670a29713c202c439df3688bca0a) fix(controller): vertex nil check for edge listing. Fixes #352 (#353)
 * [f254c28a2](https://github.com/numaproj/numaflow/commit/f254c28a20f5a1e6f70a3f78e9874f47eca39515) fix: data race in pbq manager. Closes #348 (#349)
 * [bc359457c](https://github.com/numaproj/numaflow/commit/bc359457ce221b18c46d93cc0e987f2058d59756) Chore: Windower interface. closes #234 (#340)
 * [3206bd128](https://github.com/numaproj/numaflow/commit/3206bd1282a89a8b3e760ff627a6fb4fd5dbba0d) feat: add minikube, kind and podman support (#206)
 * [d40ecdaa8](https://github.com/numaproj/numaflow/commit/d40ecdaa8deed95c22611c3086e9b8175fdc44f0) refactor: Close watermark fetcher and publisher correctly (#336)
 * [0d8f659e3](https://github.com/numaproj/numaflow/commit/0d8f659e3d83eec1b07420439299134361fe58b2) passing window information inside the context (#341)
 * [895162779](https://github.com/numaproj/numaflow/commit/89516277919a435fa5fce837cd712f734c0cae7e) feat: timestamp in UI to display milliseconds. closes #280 (#337)
 * [5c43f5aae](https://github.com/numaproj/numaflow/commit/5c43f5aaea990fe9a13b249420b29c365c1a8ce2) Simple reduce pipeline. Fixes #289 (#317)
 * [7f5d86c30](https://github.com/numaproj/numaflow/commit/7f5d86c3021e952a84c0796e8a71e970633b981c) feat: add blackhole sink. Closes #329 (#330)
 * [10f355c3c](https://github.com/numaproj/numaflow/commit/10f355c3c536a07c1e4d3cff9d27dd6101f361de) fix: move watermark based on the head of the read batch (#332)
 * [b2b975f3a](https://github.com/numaproj/numaflow/commit/b2b975f3ad1bb7d9b28c7a3b4783d620a37850f2) feat: configurable jetstream storage (#328)
 * [3fcf637ce](https://github.com/numaproj/numaflow/commit/3fcf637cebd66c0c0224a4da734191b9ad97e625) feat: support adding sidecars in vertex pods. Closes #323 (#325)
 * [6eab1b5bd](https://github.com/numaproj/numaflow/commit/6eab1b5bd334146316856bed473b2c3def4bb8eb) feat: populate watermark settings to vertex spec. Closes #320 (#321)
 * [2355978b1](https://github.com/numaproj/numaflow/commit/2355978b1ffd899cb6c70b140f1428754fc5226c) doc: add few use cases (#318)
 * [bfc1eb604](https://github.com/numaproj/numaflow/commit/bfc1eb60482ed4d3ca9c809f7aa1786a64ec487d) Chore: run in sdks-e2e tests, python-udsink log check before go-udsink (#315)
 * [cda41eca9](https://github.com/numaproj/numaflow/commit/cda41eca93e3537fa57bfa8e58c7de2579659424) fix: jetstream build watermark progressors bug (#316)
 * [bfab8f1d0](https://github.com/numaproj/numaflow/commit/bfab8f1d0c0ef9fe6ace7c6b365318bc7687ab0f) feat: update watermark offset bucket implementation (#307)
 * [1d86aa5f0](https://github.com/numaproj/numaflow/commit/1d86aa5f04b27aab132b37fa233d8fdfb81fccad) feat: shuffling support (#306)
 * [b817920ab](https://github.com/numaproj/numaflow/commit/b817920ab31630f43689ea73f7e5b43a0965a5f8) feat: customize init-container resources. Closes #303 (#304)
 * [0548d4d3b](https://github.com/numaproj/numaflow/commit/0548d4d3b4c12964b33eab70f882867d51397241) feat: watermark - remove non-share OT bucket option (#302)
 * [cc44875b2](https://github.com/numaproj/numaflow/commit/cc44875b2a2e01694ceca1fb085c3423bd330a38) feat: customization for batch jobs. Closes #259 (#300)
 * [d16015f34](https://github.com/numaproj/numaflow/commit/d16015f34d66c07149bff240b3890357fad2d436) refactor: abstract pod template (#296)
 * [4550f4591](https://github.com/numaproj/numaflow/commit/4550f45917567cb6909c4800f40610c352c7c330) feat: customization for daemon deployment. Closes #223 (#290)
 * [d61377a52](https://github.com/numaproj/numaflow/commit/d61377a52ce4fc1f7f8c5686f84f08464aca2f12) feat: add pvc support for reduce vertex PBQ (#292)
 * [b0e3f944c](https://github.com/numaproj/numaflow/commit/b0e3f944c19f20eaebb28b3e58f40cafcf9e31f7) fix(doc): hyperlink for security doc (#288)
 * [6c61728d8](https://github.com/numaproj/numaflow/commit/6c61728d8fb1eeb657ada7b7550d94ff13a51812) feat: support adding init containers to vertices. Closes #284 (#285)
 * [88cf272c4](https://github.com/numaproj/numaflow/commit/88cf272c49f5232e1f78fc12095d634b33940d3f) fix: retry when getting EOF error at E2E test (#281)
 * [1436071c8](https://github.com/numaproj/numaflow/commit/1436071c8688a51a10daecb9c972658a9cb30cfd) feat: Watermark millisecond. Fixes #201 (#278)
 * [7a7e7945e](https://github.com/numaproj/numaflow/commit/7a7e7945eef86b29703df23021203f5b5132f274) feat: add pipeline node counts and age to printcolumn. Closes #267 (#282)
 * [5883e973d](https://github.com/numaproj/numaflow/commit/5883e973d368e7f457acfd88b6ea27219e96694f) feat: introduce reduce UDF. Closes #246 (#262)
 * [a0dc17f82](https://github.com/numaproj/numaflow/commit/a0dc17f8212d34f2bc7dd5aa2bc91a3454381d64) feat: add pandoc to required tools development doc. Closes #276 (#277)
 * [284be2d69](https://github.com/numaproj/numaflow/commit/284be2d692c3c64dc104d3587afedc3d4473d37b) feat: add isbsvc type and age to printcolumn. Closes #268 (#275)
 * [7bb689bc0](https://github.com/numaproj/numaflow/commit/7bb689bc00aae21c3e77dceb6254b55040c60324) fix: watermark consumer fix (#273)
 * [8ff9e28e4](https://github.com/numaproj/numaflow/commit/8ff9e28e4071749b14c4ab8d9043d2b97da94714) refactor: generalize watermark fetching as an interface of ISB service. Fixes #252 (#263)
 * [8e038d1ee](https://github.com/numaproj/numaflow/commit/8e038d1ee301936cdcf2d7f4355199ba641725c4) fix: set default property values for minimal CRD installation (#264)
 * [57df392f2](https://github.com/numaproj/numaflow/commit/57df392f227eab168d36907aa5f4099274c29f48) fix: validate only one isbsvc implementation type is defined. Fixes #269 (#271)
 * [21378a361](https://github.com/numaproj/numaflow/commit/21378a3615184395bda26b05d0a2be10f104dcc8) fix: main branch make build failure: math.MaxInt64 for int type (#265)
 * [3d9997d65](https://github.com/numaproj/numaflow/commit/3d9997d6582b6fca215631cb611cbeb2eebaf7a4) fix: nil pointer deref when running example with minimal CRD. Fixes #260 (#261)
 * [4b0cbc376](https://github.com/numaproj/numaflow/commit/4b0cbc376f09380ce193ed42550589e17d964936) fix: retry only the failed offsets (#255)
 * [27e6a8755](https://github.com/numaproj/numaflow/commit/27e6a8755bd1092b31d93fac891d1decd006a093) fix: re-enable build constraint on processor manager test. Fixes #256 (#257)
 * [98b3ec4d8](https://github.com/numaproj/numaflow/commit/98b3ec4d82315a62512c5750610fad7f73f17880) fix: container resource for jetstream isbsvc. Fixes #253 (#254)
 * [e615e16ed](https://github.com/numaproj/numaflow/commit/e615e16edf05f2cd32c75ef43d2d3a4d1a6bd541) fix: update vertex watermark fetching logic. Fixes: #134 (#245)
 * [30c734bd0](https://github.com/numaproj/numaflow/commit/30c734bd0f2b3698e5c86e6d41ce33164edbc69d) fix: watermark watcher leak (#242)
 * [3d29f79d5](https://github.com/numaproj/numaflow/commit/3d29f79d5dda7320048bff5662064d2a449ed6f9) fix(docs): fix a typo (#241)
 * [0370fd6c9](https://github.com/numaproj/numaflow/commit/0370fd6c91ef68c6300fbfba54f537b0742124a9) feat: Support running UX server with namespace scope. Fixes #248 (#249)
 * [29f15d57a](https://github.com/numaproj/numaflow/commit/29f15d57a125c54b6de8c4cd29d16e3d8473b655) fix(manifests): Include ServiceAccount in namespace scoped install (#240)
 * [6870d2a45](https://github.com/numaproj/numaflow/commit/6870d2a4558107cf824e5ba91ff8c65125a43eed) fix: Watermark close fix and removed the nil check (#238)
 * [998e3988c](https://github.com/numaproj/numaflow/commit/998e3988c38c37aab252cce3176b24999f54ab97) fix: skip publishing watermarks to unexpected vertices. Fixes #235 (#236)
 * [fff05f32a](https://github.com/numaproj/numaflow/commit/fff05f32a137ed949718a6cfbb2afa5d0dac4b5d) fix: update default watermark to -1. Fixes #133 (#218)
 * [a23e35920](https://github.com/numaproj/numaflow/commit/a23e35920598f4735eb578c8898fe4fe57f02d07) feat: support disabling TLS and changing port for UX server (#228)
 * [5a4387c7c](https://github.com/numaproj/numaflow/commit/5a4387c7ce55947842dd0f58a06769a986a7f885) feat: reducer for stream aggregation without fault tolerance (#208)
 * [fc2ba4e97](https://github.com/numaproj/numaflow/commit/fc2ba4e97848578aaa48e62f87bb208574b05cf3) feat: in-memory watermark store for better testing (#216)
 * [c89aef312](https://github.com/numaproj/numaflow/commit/c89aef31286be5f0a7ac0fce205e389410ba12e6) Add USERS.md (#221)
 * [2377c4c69](https://github.com/numaproj/numaflow/commit/2377c4c69b2255b4e3005f562eb2f1b8161f7b55) fix(watermark): generator should not publish wm for every message (#217)

### Contributors

 * David Seapy
 * Derek Wang
 * Ed Lee
 * Juanlu Yu
 * Keran Yang
 * Shay Dratler
 * SianLoong
 * Vedant Gupta
 * Vigith Maurice
 * Yashash H L
 * ashwinidulams
 * dependabot[bot]
 * xdevxy

## v0.6.5 (2022-12-07)

 * [845c9594e](https://github.com/numaproj/numaflow/commit/845c9594e026dbaa22f139cd20a9637236d95deb) Update manifests to v0.6.5
 * [676ea1c60](https://github.com/numaproj/numaflow/commit/676ea1c603a9d49f24449e27db91367c894b2a08) fix: adding lock while discovering partitions, Closes #412 (#413)
 * [c439a6a18](https://github.com/numaproj/numaflow/commit/c439a6a1851101e60b84bd23d323320abbc5fac8) fix(test): e2e-api-pod can not start on M1 mac (#410)
 * [115a69d69](https://github.com/numaproj/numaflow/commit/115a69d69d82221e924f86c36af9f88ac49dc108) fix: getWatermark to return-1 if any processor returns -1  (#402)
 * [e6e24eef6](https://github.com/numaproj/numaflow/commit/e6e24eef6c0ba8725ad87d24bcaf2cb427784485) fix: e2e testing for PBQ WAL with reduce pipeline (#393)
 * [7ef3d47c8](https://github.com/numaproj/numaflow/commit/7ef3d47c8cb113fa6c1fb46186b8e74874358a00) feat: add Grafana instruction and a dashboard template. Closes #287 (#381)
 * [13ce4d279](https://github.com/numaproj/numaflow/commit/13ce4d279e93b1cbcc82f768b29232b7a3d82a67) fix: unit tests for replay. Closes #373 (#377)

### Contributors

 * Derek Wang
 * Keran Yang
 * Yashash H L
 * xdevxy

## v0.6.4 (2022-11-28)

 * [ad9719a61](https://github.com/numaproj/numaflow/commit/ad9719a61ec7208da36a228cea129f65cdf70d77) Update manifests to v0.6.4
 * [c5e82176b](https://github.com/numaproj/numaflow/commit/c5e82176b1dd70aa991797bffaaf02bc4a8f6609) chore(docs): update docs (#380)
 * [1b244c1a0](https://github.com/numaproj/numaflow/commit/1b244c1a0eea48ab9c75a0704cf70848640e5d6b) fix: best effort processing during SIGTERM. Closes #371 (#372)
 * [9bb8ebd58](https://github.com/numaproj/numaflow/commit/9bb8ebd58835618d62402f11a191063cb299170c) feat(wal): First pass to implement WAL and hook to PBQ store. (#344)

### Contributors

 * Derek Wang
 * Vigith Maurice
 * xdevxy

## v0.6.3 (2022-11-18)

 * [3cf391b19](https://github.com/numaproj/numaflow/commit/3cf391b19e862744ae04ef350e34dc57d88fe6b1) Update manifests to v0.6.3
 * [bec020b78](https://github.com/numaproj/numaflow/commit/bec020b78f7c1aa5cec4e0c6503beadb71955465) feat: watermark otwatcher enhancement (#364)
 * [2b5478fca](https://github.com/numaproj/numaflow/commit/2b5478fca9cd5781593750c82ec29d3cd0a65b85) refactor(docs): group docs in categories (#362)
 * [6d9e129b5](https://github.com/numaproj/numaflow/commit/6d9e129b5fa374c0405d4fce18d22eb351acc488) chore(deps): bump loader-utils from 2.0.3 to 2.0.4 in /ui (#356)
 * [77364a4df](https://github.com/numaproj/numaflow/commit/77364a4df0114f2d8b296701a6b9e85f2f1041a7) fix(controller): vertex nil check for edge listing. Fixes #352 (#353)
 * [7db4fe562](https://github.com/numaproj/numaflow/commit/7db4fe562bd0e9a1034c7d02345251089827e20b) fix: data race in pbq manager. Closes #348 (#349)
 * [6f5e83a70](https://github.com/numaproj/numaflow/commit/6f5e83a70a2b97ec87bfb416abe83490393eb179) Chore: Windower interface. closes #234 (#340)
 * [24ba51570](https://github.com/numaproj/numaflow/commit/24ba5157085da3c55d3a0cde617e2ccfc8b7346e) feat: add minikube, kind and podman support (#206)
 * [12c6ca527](https://github.com/numaproj/numaflow/commit/12c6ca527d81d14fd3f9c7b20dbe3bf2795cc08e) refactor: Close watermark fetcher and publisher correctly (#336)
 * [2a8b97e15](https://github.com/numaproj/numaflow/commit/2a8b97e1551f5f2c6e1c447ec7b23f10a64801c3) passing window information inside the context (#341)
 * [d52a5a75d](https://github.com/numaproj/numaflow/commit/d52a5a75d5eb73fe306ea62f387e00ad7e1f7acf) feat: timestamp in UI to display milliseconds. closes #280 (#337)
 * [de9059cf3](https://github.com/numaproj/numaflow/commit/de9059cf3884d3fe2add9ec7dd2164509400ea0c) Simple reduce pipeline. Fixes #289 (#317)
 * [3d936a506](https://github.com/numaproj/numaflow/commit/3d936a5061f1ad1542c9429f8cd2351dee469a99) feat: add blackhole sink. Closes #329 (#330)
 * [45905475d](https://github.com/numaproj/numaflow/commit/45905475dd0da7dfa29c75c905a33a49f9f47e73) fix: move watermark based on the head of the read batch (#332)
 * [049e5c66b](https://github.com/numaproj/numaflow/commit/049e5c66bd95d15fa79066ee66edd222b6af8b1d) feat: configurable jetstream storage (#328)
 * [ee5cd6425](https://github.com/numaproj/numaflow/commit/ee5cd6425b1ec12f37332159fcdf06b12d38907b) feat: support adding sidecars in vertex pods. Closes #323 (#325)

### Contributors

 * David Seapy
 * Derek Wang
 * Juanlu Yu
 * Shay Dratler
 * Vedant Gupta
 * Vigith Maurice
 * Yashash H L
 * ashwinidulams
 * dependabot[bot]

## v0.6.2 (2022-11-07)

 * [99be6c088](https://github.com/numaproj/numaflow/commit/99be6c088a8dee0ae7ff74a00fc991f4009beaa7) Update manifests to v0.6.2
 * [dc733da11](https://github.com/numaproj/numaflow/commit/dc733da118b2683c4f3359763b9252d7fa11785a) feat: populate watermark settings to vertex spec. Closes #320 (#321)
 * [2b247cad0](https://github.com/numaproj/numaflow/commit/2b247cad0e560accf14467407b7aada27610f7bc) doc: add few use cases (#318)
 * [07ffa168f](https://github.com/numaproj/numaflow/commit/07ffa168f49210d86c11cc982e5018e9a4afb5e2) Chore: run in sdks-e2e tests, python-udsink log check before go-udsink (#315)
 * [7b3285b95](https://github.com/numaproj/numaflow/commit/7b3285b956bc97e892ad4107f5808e2aa3a9fca6) fix: jetstream build watermark progressors bug (#316)
 * [1198a6097](https://github.com/numaproj/numaflow/commit/1198a6097f97817d7462b741cdc789c625424ffe) feat: update watermark offset bucket implementation (#307)
 * [34a6d7095](https://github.com/numaproj/numaflow/commit/34a6d7095229509deff1503b6d5fd62b3a2cf93f) feat: shuffling support (#306)
 * [448127ff0](https://github.com/numaproj/numaflow/commit/448127ff0c47db9f5afd502d5df846a44d536be1) feat: customize init-container resources. Closes #303 (#304)
 * [61cf22723](https://github.com/numaproj/numaflow/commit/61cf22723a43b5c48df9c1dea3e39cefd6481182) feat: watermark - remove non-share OT bucket option (#302)
 * [51c9ff42b](https://github.com/numaproj/numaflow/commit/51c9ff42ba520c2b39e5568c97810e98177a56cb) feat: customization for batch jobs. Closes #259 (#300)
 * [afbe25576](https://github.com/numaproj/numaflow/commit/afbe2557691e41a2487591c9b732814494544e12) refactor: abstract pod template (#296)
 * [240894606](https://github.com/numaproj/numaflow/commit/240894606a5e5968a388ff4c3c8da29e187f73cf) feat: customization for daemon deployment. Closes #223 (#290)
 * [bedf567cf](https://github.com/numaproj/numaflow/commit/bedf567cf17573421be6d2f2c19e54941c33fe97) feat: add pvc support for reduce vertex PBQ (#292)
 * [2341614b2](https://github.com/numaproj/numaflow/commit/2341614b2e9ad5fbc9ba9d1a76531bf5e84250d6) fix(doc): hyperlink for security doc (#288)
 * [6c05190d3](https://github.com/numaproj/numaflow/commit/6c05190d31167a0d1cbf938633ee2225512b5de1) feat: support adding init containers to vertices. Closes #284 (#285)
 * [dc96b8720](https://github.com/numaproj/numaflow/commit/dc96b8720d009e985f016c1eff45ec465fe22d7a) fix: retry when getting EOF error at E2E test (#281)
 * [f5db937cc](https://github.com/numaproj/numaflow/commit/f5db937ccd4c5f6a2d873c24b1c78b314dc35046) feat: Watermark millisecond. Fixes #201 (#278)
 * [c15353650](https://github.com/numaproj/numaflow/commit/c153536500f3983016ffd91883cd837f34645679) feat: add pipeline node counts and age to printcolumn. Closes #267 (#282)

### Contributors

 * David Seapy
 * Derek Wang
 * Juanlu Yu
 * Keran Yang
 * Vigith Maurice

## v0.6.1 (2022-10-26)

 * [32b284f62](https://github.com/numaproj/numaflow/commit/32b284f626aaffdfc16c267a3890e41cdc5f0142) Update manifests to v0.6.1
 * [9684e1616](https://github.com/numaproj/numaflow/commit/9684e1616e97ae4eeb791d7a9164d2f31e9317a4) fix(manifests): Include ServiceAccount in namespace scoped install (#240)
 * [fe83918a4](https://github.com/numaproj/numaflow/commit/fe83918a458d632af71daccff56bf7d00aaaa012) fix(docs): fix a typo (#241)
 * [f2094b4ba](https://github.com/numaproj/numaflow/commit/f2094b4baa031f985068f5accf9426a599b72f97) feat: introduce reduce UDF. Closes #246 (#262)
 * [e19a1e7d9](https://github.com/numaproj/numaflow/commit/e19a1e7d90644378201eb8854b226b5c50c6cf9c) feat: add pandoc to required tools development doc. Closes #276 (#277)
 * [9a937118f](https://github.com/numaproj/numaflow/commit/9a937118fd6ffcb5ff19803423019ede70e20d4b) feat: add isbsvc type and age to printcolumn. Closes #268 (#275)
 * [f25e303e7](https://github.com/numaproj/numaflow/commit/f25e303e773d4a0e1f3d32815f839628e35278f0) fix: watermark consumer fix (#273)
 * [d2a3d9082](https://github.com/numaproj/numaflow/commit/d2a3d90823046b103b9729b142903ba9f1903bf4) refactor: generalize watermark fetching as an interface of ISB service. Fixes #252 (#263)
 * [5ffcadcca](https://github.com/numaproj/numaflow/commit/5ffcadccafeef5711f784acb005c51051c06fd18) fix: set default property values for minimal CRD installation (#264)
 * [17a995645](https://github.com/numaproj/numaflow/commit/17a99564587bf4cc68d057bccd808ea611b1bf7d) fix: validate only one isbsvc implementation type is defined. Fixes #269 (#271)
 * [2272a1fcb](https://github.com/numaproj/numaflow/commit/2272a1fcbb8c0fdb676093eba1b1e27e13fef257) fix: main branch make build failure: math.MaxInt64 for int type (#265)
 * [02c31d277](https://github.com/numaproj/numaflow/commit/02c31d277b0a74f6e97aefd88c2e11f32d7b4f95) fix: nil pointer deref when running example with minimal CRD. Fixes #260 (#261)
 * [391b53e12](https://github.com/numaproj/numaflow/commit/391b53e1203d0989f71bcad4840446e0dda55324) fix: retry only the failed offsets (#255)
 * [7b42dc80c](https://github.com/numaproj/numaflow/commit/7b42dc80c813c694ba494cb6f6b86347745e5b7b) fix: re-enable build constraint on processor manager test. Fixes #256 (#257)
 * [343604901](https://github.com/numaproj/numaflow/commit/3436049011b6ed245b048ec81a9185cde1e48e62) fix: container resource for jetstream isbsvc. Fixes #253 (#254)
 * [33ce74222](https://github.com/numaproj/numaflow/commit/33ce74222a76dc8deeb1306da2165d90571fdba1) fix: update vertex watermark fetching logic. Fixes: #134 (#245)
 * [fd219a5cd](https://github.com/numaproj/numaflow/commit/fd219a5cda3623549abf47e064a4549470056b59) fix: watermark watcher leak (#242)
 * [979a3a3f8](https://github.com/numaproj/numaflow/commit/979a3a3f8d0649cb6ac82722513a3c96827bfb70) feat: Support running UX server with namespace scope. Fixes #248 (#249)
 * [5e9d1c1ce](https://github.com/numaproj/numaflow/commit/5e9d1c1ceb88b3ad9cab2a947018e8935cbbd73f) fix: Watermark close fix and removed the nil check (#238)
 * [340bd820d](https://github.com/numaproj/numaflow/commit/340bd820ddd25d507541dcd368ce1eaf51ecc9e3) fix: skip publishing watermarks to unexpected vertices. Fixes #235 (#236)
 * [904b2cde5](https://github.com/numaproj/numaflow/commit/904b2cde562351fd39cc54b6ac0c91baa9ab3047) fix: update default watermark to -1. Fixes #133 (#218)
 * [321e285fa](https://github.com/numaproj/numaflow/commit/321e285fa437a0fbf33cb127ce6abbe1feaf0159) feat: support disabling TLS and changing port for UX server (#228)
 * [d0d74e197](https://github.com/numaproj/numaflow/commit/d0d74e19745084dbb677a56d04205ddff435d427) feat: reducer for stream aggregation without fault tolerance (#208)
 * [06a9b58a3](https://github.com/numaproj/numaflow/commit/06a9b58a3e3b8452e767f2cc34229b5ee0145aad) feat: in-memory watermark store for better testing (#216)
 * [f25cc58e6](https://github.com/numaproj/numaflow/commit/f25cc58e6b9fc504c3b6a15cbd503f479f60df1d) Add USERS.md (#221)
 * [a37cece93](https://github.com/numaproj/numaflow/commit/a37cece931629f230ad9c981351641c23dfdb3f0) fix(watermark): generator should not publish wm for every message (#217)

### Contributors

 * David Seapy
 * Derek Wang
 * Ed Lee
 * Juanlu Yu
 * Keran Yang
 * SianLoong
 * Yashash H L
 * ashwinidulams

## v0.6.0 (2022-10-12)

 * [48aad5fcb](https://github.com/numaproj/numaflow/commit/48aad5fcbf855380b06f90c082e92571916e4c54) Update manifests to v0.6.0
 * [09ce54f10](https://github.com/numaproj/numaflow/commit/09ce54f1008d5a822045b53779b1722aa503700f) fix(autoscaling): Ack pending should be included in total pending calculation (#212)
 * [9922787ce](https://github.com/numaproj/numaflow/commit/9922787ce1d00b97ac119081a303ae26d8281cb8) fix(autoscaling): Skip autoscaling if vertex is not in running phase (#207)
 * [bc2380a7b](https://github.com/numaproj/numaflow/commit/bc2380a7b6a035f14fbffe0a0cbfe613056e6b93) feat: ISBSVC add support for redis cluster mode (#195)
 * [72a96a584](https://github.com/numaproj/numaflow/commit/72a96a5843bc7dbcde4a092cbfc8e771d0e70bef) refactor: move controllers package to pkg/reconciler (#192)
 * [b1b78faaf](https://github.com/numaproj/numaflow/commit/b1b78faafd0102f6c19d4905b60be4f4d97153ad) fix: update udf fetchWatermark and publishWatermark initial values (#193)
 * [d49126006](https://github.com/numaproj/numaflow/commit/d491260060ae25d57b42b7df76781b34437cf355) fix(docs): readme for UI development (#181)
 * [6b121c6e4](https://github.com/numaproj/numaflow/commit/6b121c6e4f87acaea2a7828511c187f5508ea62a) feat: grpc udsink (#174)
 * [567da7b0d](https://github.com/numaproj/numaflow/commit/567da7b0d0235a171f7c7b3bdefb5b8b1ca5acb3) fix: numaflow-go udf example & docs (#177)
 * [4652f808a](https://github.com/numaproj/numaflow/commit/4652f808a35b73c38f22d8d03df46405959198cd) fix: use scale.max if it is set (#179)
 * [900314bc6](https://github.com/numaproj/numaflow/commit/900314bc6c01e647b53c4fa916fd65dfd0ded221) fix broken path (#176)
 * [3b02f2a6a](https://github.com/numaproj/numaflow/commit/3b02f2a6a0e1a0e8bd03479eadb0f713a4de09fb) feat: Shuffle implementation (#169)
 * [021bb9dfd](https://github.com/numaproj/numaflow/commit/021bb9dfdb4a3d8da44275397f06d022b0edcfc4) feat: windowing operations (#157)
 * [7d411294a](https://github.com/numaproj/numaflow/commit/7d411294a28cd9aa135efe3de14405ebc637e73c) feat: watermark for sources (#159)
 * [5f5b2dfdc](https://github.com/numaproj/numaflow/commit/5f5b2dfdc4d999b65d4a3b7a5366e3234677bb61) fix: daemon service client memory leak (#161)
 * [bfe966956](https://github.com/numaproj/numaflow/commit/bfe966956cc9ea36759c62f5a787c0e1ed98fb68) pbq implementation (#155)
 * [8dfedd838](https://github.com/numaproj/numaflow/commit/8dfedd838794d2bee94b18f720057ef9e99b73e0) feat: check if udf is running in liveness probe  (#156)
 * [81e76d82e](https://github.com/numaproj/numaflow/commit/81e76d82e375c4f5e9f392cf53a2624fc036878d) feat: Add udf grpc support Fixes #145  (#146)
 * [511faffcb](https://github.com/numaproj/numaflow/commit/511faffcb7ee6860d15d97dd54332a14300d88f8) refactor: some refactor on watermark (#149)
 * [7fe40c428](https://github.com/numaproj/numaflow/commit/7fe40c428c50af435b54be8d512cae40b6b7e49e) fix: Fixed JS bug (#144)
 * [24a16a049](https://github.com/numaproj/numaflow/commit/24a16a049f2f3a1752ee702f5020136a51d66e69) bug: watermark needs nil check
 * [f4ed831ba](https://github.com/numaproj/numaflow/commit/f4ed831ba0d0cbde92f1b7cc1113c83a0c77b702) fix: pipeline UI broken when vertex scaling down to 0 (#132)
 * [0ae0377f6](https://github.com/numaproj/numaflow/commit/0ae0377f6019d42dfd0625d86304299061cb18c8) feat: JetStream auto-reconnection (#127)
 * [2fc04eb3b](https://github.com/numaproj/numaflow/commit/2fc04eb3b87f42526c21c8648e7aa89a20c933f1) feat: Add watermark for sink vertex (#124)
 * [d958ee6de](https://github.com/numaproj/numaflow/commit/d958ee6defda7e37dcf2192609634bb4d5f97be1) feat: autoscaling with back pressure factor (#123)
 * [b1f776821](https://github.com/numaproj/numaflow/commit/b1f776821e10231292fbc50dce1639fd492a61af) feat: add watermark to UI (#122)
 * [7feeaa879](https://github.com/numaproj/numaflow/commit/7feeaa87996dc173438b0259902266e66d05077b) feat: add processing rate to UI (#121)
 * [43fae931e](https://github.com/numaproj/numaflow/commit/43fae931e947c90e3a15c62f65d9cacaf48bbcfa) feat: Expose watermark over HTTP (#120)
 * [ec02304a1](https://github.com/numaproj/numaflow/commit/ec02304a15e7436b0559f6443b2ab86d186067fe) fix: daemon service rest api not working (#119)
 * [f3da56d36](https://github.com/numaproj/numaflow/commit/f3da56d36c23c9e356aa689d81348e7e21801d90) chore(deps): bump terser from 5.14.1 to 5.14.2 in /ui (#117)
 * [e2e63c84c](https://github.com/numaproj/numaflow/commit/e2e63c84c77eb874629b5f547a2383da2f96e7d8) feat: Numaflow autoscaling (#115)
 * [e5da3f544](https://github.com/numaproj/numaflow/commit/e5da3f544e2f755d608c73d98c0aed108b813197) feat: watermark for headoffset (#116)
 * [a45b2eed1](https://github.com/numaproj/numaflow/commit/a45b2eed124248a28460309a3ea472769c7562ef) feat: support namespace scope installation (#112)
 * [ce39199e7](https://github.com/numaproj/numaflow/commit/ce39199e76150fb1c88bfad35c92e57c23ea4b3a) feat: Expose ReadTimeoutSeconds on Vertex (#110)
 * [18ad1c5fb](https://github.com/numaproj/numaflow/commit/18ad1c5fbe2305632011e67d6e239cc8ab1f8f97) fix: imagepullpocily for local testing (#113)
 * [469849b5b](https://github.com/numaproj/numaflow/commit/469849b5b9c29889ee38f0712ad2267088bdda5c) feat: removed udfWorkers from limits and added some docs (#103)
 * [3fada6673](https://github.com/numaproj/numaflow/commit/3fada667357ae9ead741ad24e7ba33b7cebcbf99) feat: Add icon and other minor changes (#94)
 * [a81838d7b](https://github.com/numaproj/numaflow/commit/a81838d7baa0f0b5001aa38cb2e6627bf9b2d977) feat: end to end tickgen watermark validation (#98)
 * [d7d93175a](https://github.com/numaproj/numaflow/commit/d7d93175a93104e28e375d61e2dd33669764ef42) fix: Broken hyperlink (#96)
 * [a2e079264](https://github.com/numaproj/numaflow/commit/a2e079264dfc87b65018c57723119ccbe512c99a) add no-op KV Store (#91)
 * [45c8cb69d](https://github.com/numaproj/numaflow/commit/45c8cb69dc9ebe2352f58e7cd71eb798bc542384) feat: no operation watermark progressor (#90)
 * [448c229ab](https://github.com/numaproj/numaflow/commit/448c229ab7d399505b616a65a9916cac00db3f4d) feat: kafka source pending messages (#80)
 * [1aa39300e](https://github.com/numaproj/numaflow/commit/1aa39300ec8c46e262d9203dbaa4b6c4d72490ce) feat: Interface for Watermark (#82)
 * [be78c5237](https://github.com/numaproj/numaflow/commit/be78c5237358fe6325a3a2609ea4597ea51257ab) feat: expose pending messages and processing rate (#79)
 * [df30f2a84](https://github.com/numaproj/numaflow/commit/df30f2a84d13d535b226aaf0feb017ceb1952664) feat: Added the right way to decipher from and to vertex (#78)
 * [639c459ac](https://github.com/numaproj/numaflow/commit/639c459ac2ad92adabe618ad162d18dab45f5858) feat: define buffer limits on edges (#70)
 * [41fdd38bd](https://github.com/numaproj/numaflow/commit/41fdd38bd102ad91ad75e9d8a260a818762ec91d) feat: Merge UX server code (#67)
 * [ced990790](https://github.com/numaproj/numaflow/commit/ced9907908b3230f7f909341401d8d4934381240) feat: auto-scaling (part 1) (#59)
 * [fd5b37412](https://github.com/numaproj/numaflow/commit/fd5b37412ab0cb66f2399f90b856b960e570e368) Added name to service spec (#58)
 * [dc2badfdc](https://github.com/numaproj/numaflow/commit/dc2badfdc046c214aef71f32a9cd3a60038bff41) feat: introduce source buffer and sink buffer (#53)
 * [4ed83a2ae](https://github.com/numaproj/numaflow/commit/4ed83a2aede73510da17aab5431c9e3e549a5d47) feat: async publishing for kafka sink (#51)
 * [9f9f5ba73](https://github.com/numaproj/numaflow/commit/9f9f5ba73a4bcfb766085c349dffdde15ce32135) fix spelling errors (#48)
 * [f423002ef](https://github.com/numaproj/numaflow/commit/f423002efb8f307c995bbf59e63fb7bc52d85d31) feat: controller to create buckets (#47)
 * [8328739c6](https://github.com/numaproj/numaflow/commit/8328739c6534473a3892aeaedc4261b43449efc4) turn on watermark only if ENV value is true (#46)
 * [46f72e237](https://github.com/numaproj/numaflow/commit/46f72e237d5a153be6c59bd736130fe70abaf1e0) minimal end to end line-graph watermark integration (#43)
 * [1f8203f4a](https://github.com/numaproj/numaflow/commit/1f8203f4ad13ae4e9713373d7858f0096957c93e) Fixed spelling error (#44)
 * [f1e99eae3](https://github.com/numaproj/numaflow/commit/f1e99eae3b9e5a49cc651f8ef47a912329549960) Exponential buckets (#42)
 * [dfcfdeba8](https://github.com/numaproj/numaflow/commit/dfcfdeba8b2c86661147ef53707aa2d3f46c5074) fix: different behavior for time.After in go 1.18 (#39)

### Contributors

 * Chrome
 * Derek Wang
 * Juanlu Yu
 * Krithika3
 * Qianbo Huai
 * Saravanan Balasubramanian
 * Sidhant Kohli
 * Vigith Maurice
 * Yashash H L
 * dependabot[bot]

## v0.5.6 (2022-09-19)

 * [ac15d229a](https://github.com/numaproj/numaflow/commit/ac15d229af7ba127c162815d83508bb62b6b35b5) Update manifests to v0.5.6
 * [f2363757f](https://github.com/numaproj/numaflow/commit/f2363757fca2b51cc466afe344fb54215c4c5051) feat: grpc udsink (#174)
 * [2650c2de5](https://github.com/numaproj/numaflow/commit/2650c2de59f1903a269dd3c15af0e0c285e5d290) fix: numaflow-go udf example & docs (#177)
 * [c44f733f2](https://github.com/numaproj/numaflow/commit/c44f733f2b20c1e2d75664f4b71066b35ea6bc3b) fix: use scale.max if it is set (#179)
 * [39e92d063](https://github.com/numaproj/numaflow/commit/39e92d06380cba339f692251d7ca319b5fc481cb) fix broken path (#176)
 * [46ce0f879](https://github.com/numaproj/numaflow/commit/46ce0f879758b38153d7d4a58c00e8714ce8871d) feat: Shuffle implementation (#169)
 * [71ca00a1b](https://github.com/numaproj/numaflow/commit/71ca00a1b425706d7ef7bbc6f5f19ff8f2718305) feat: windowing operations (#157)
 * [ca00b78f2](https://github.com/numaproj/numaflow/commit/ca00b78f2d8ece0bfee426ea2f2b2216fa24f127) feat: watermark for sources (#159)
 * [91e21ceec](https://github.com/numaproj/numaflow/commit/91e21ceec78ced5ad53979c36b70be39198a5af5) pbq implementation (#155)
 * [654240042](https://github.com/numaproj/numaflow/commit/654240042a1d302166fdbc1ac367c92bc052b19a) feat: check if udf is running in liveness probe  (#156)
 * [79dce0b38](https://github.com/numaproj/numaflow/commit/79dce0b386d7fec7a008a1d0a1e4d7bf9835ecaa) feat: Add udf grpc support Fixes #145  (#146)

### Contributors

 * Chrome
 * Derek Wang
 * Juanlu Yu
 * Vigith Maurice
 * Yashash H L

## v0.5.5 (2022-09-07)

 * [9aae638c4](https://github.com/numaproj/numaflow/commit/9aae638c4bc0011e027b40c4b7a3b17b189ea945) Update manifests to v0.5.5
 * [324143252](https://github.com/numaproj/numaflow/commit/324143252c3657b85c643b074949679dcd4f26ee) fix: daemon service client memory leak (#161)
 * [be47a26af](https://github.com/numaproj/numaflow/commit/be47a26af5965897951da35a53eff6d5f423df89) refactor: some refactor on watermark (#149)
 * [857cce75b](https://github.com/numaproj/numaflow/commit/857cce75b86d7aa96cdeebbf822bfb52607e05da) fix: Fixed JS bug (#144)
 * [da16abc7e](https://github.com/numaproj/numaflow/commit/da16abc7e623d0e25564fdb64cc6c3f01c23e88d) bug: watermark needs nil check
 * [c9998a1ca](https://github.com/numaproj/numaflow/commit/c9998a1ca3926f37d2180e1082beacdb24d0b3b1) fix: pipeline UI broken when vertex scaling down to 0 (#132)

### Contributors

 * Derek Wang
 * Krithika3
 * Vigith Maurice

## v0.5.4 (2022-08-05)

 * [57513b408](https://github.com/numaproj/numaflow/commit/57513b408eddd8e7918cab540ea866ad19d13518) Update manifests to v0.5.4
 * [94cdb82fe](https://github.com/numaproj/numaflow/commit/94cdb82febe92021a37ea44c466949982da13910) feat: JetStream auto-reconnection (#127)
 * [8d8354082](https://github.com/numaproj/numaflow/commit/8d8354082966a524275539dfc2c31e2c2a2c47bc) feat: Add watermark for sink vertex (#124)
 * [228ba3216](https://github.com/numaproj/numaflow/commit/228ba3216bdfa1b667407be8310e5561f5fea90e) feat: autoscaling with back pressure factor (#123)
 * [9833efdf1](https://github.com/numaproj/numaflow/commit/9833efdf14892f20ea792a042d03adec4ad3a91a) feat: add watermark to UI (#122)
 * [0dab55d87](https://github.com/numaproj/numaflow/commit/0dab55d8707a7172b37e4e59053ea0d770520982) feat: add processing rate to UI (#121)
 * [ffd38a152](https://github.com/numaproj/numaflow/commit/ffd38a1528214f2a09986459f3a14588276fbbe0) feat: Expose watermark over HTTP (#120)
 * [c09502a28](https://github.com/numaproj/numaflow/commit/c09502a286b8f35ede0ccc43545afc391d967e58) fix: daemon service rest api not working (#119)
 * [ebc10f411](https://github.com/numaproj/numaflow/commit/ebc10f4117d5ee19dbe6ad4915b7f63f14325373) chore(deps): bump terser from 5.14.1 to 5.14.2 in /ui (#117)
 * [84490ca85](https://github.com/numaproj/numaflow/commit/84490ca852f95f661fbe93687180672ad5ecacca) feat: Numaflow autoscaling (#115)
 * [32b98486f](https://github.com/numaproj/numaflow/commit/32b98486f4c7f004ed4b36be5f4af18e29d71969) feat: watermark for headoffset (#116)
 * [283dae907](https://github.com/numaproj/numaflow/commit/283dae9073f59de35b82ba3b4d243204d9d77067) feat: support namespace scope installation (#112)
 * [8e612b1fa](https://github.com/numaproj/numaflow/commit/8e612b1fa2b3ca3c0ad037ab816b7ddc1322dd7d) feat: Expose ReadTimeoutSeconds on Vertex (#110)
 * [d95d41bd6](https://github.com/numaproj/numaflow/commit/d95d41bd6446cd0b1312b93da4a88dd305b29ce4) fix: imagepullpocily for local testing (#113)

### Contributors

 * Derek Wang
 * Krithika3
 * Saravanan Balasubramanian
 * Sidhant Kohli
 * Vigith Maurice
 * dependabot[bot]

## v0.5.3 (2022-07-08)

 * [efee5442c](https://github.com/numaproj/numaflow/commit/efee5442c7618959319a1825f467f059fe67ac57) Update manifests to v0.5.3
 * [5895facd7](https://github.com/numaproj/numaflow/commit/5895facd75b3fe2ba296a7283bca61e9b7b9e4e5) feat: removed udfWorkers from limits and added some docs (#103)
 * [0b75495f1](https://github.com/numaproj/numaflow/commit/0b75495f1b5795de619cb19430d2b125457e119a) feat: Add icon and other minor changes (#94)
 * [7eb08f58b](https://github.com/numaproj/numaflow/commit/7eb08f58b7730357ed5c827c45ceda6177f5cc37) feat: end to end tickgen watermark validation (#98)
 * [3338e6589](https://github.com/numaproj/numaflow/commit/3338e6589c7910b30813e9a9912916956f1d3a7e) fix: Broken hyperlink (#96)
 * [e31122293](https://github.com/numaproj/numaflow/commit/e311222937098b49d17f2167f9002adefa1e2461) add no-op KV Store (#91)
 * [5d2f90ed0](https://github.com/numaproj/numaflow/commit/5d2f90ed0af7adbd2e2ddffd96b71577ce78e604) feat: no operation watermark progressor (#90)
 * [f58d0f498](https://github.com/numaproj/numaflow/commit/f58d0f4989a1569e45a6b66056e99db46f2b3218) feat: kafka source pending messages (#80)
 * [cbb16ca23](https://github.com/numaproj/numaflow/commit/cbb16ca23db1054c8870b279fad47c183e5ad76a) feat: Interface for Watermark (#82)
 * [5592bb1b4](https://github.com/numaproj/numaflow/commit/5592bb1b453bf00be2c756487614700820a6c95f) feat: expose pending messages and processing rate (#79)
 * [06a3df2d3](https://github.com/numaproj/numaflow/commit/06a3df2d310f8d52bcb062c7d3e6a249723796d5) feat: Added the right way to decipher from and to vertex (#78)
 * [a0908ad49](https://github.com/numaproj/numaflow/commit/a0908ad49a356b7f1cd40d40e5e8efd7f1994205) feat: define buffer limits on edges (#70)
 * [a1d363955](https://github.com/numaproj/numaflow/commit/a1d363955b4c4a2ff9bddb821e43a955e370fc4c) feat: Merge UX server code (#67)
 * [571c48eb0](https://github.com/numaproj/numaflow/commit/571c48eb039bc8b4e27e87b8f959aa2d72f56f23) feat: auto-scaling (part 1) (#59)
 * [1e0384ba7](https://github.com/numaproj/numaflow/commit/1e0384ba76994b68cb75a0967cb1e0460bc19b75) Added name to service spec (#58)

### Contributors

 * Derek Wang
 * Krithika3
 * Sidhant Kohli
 * Vigith Maurice

## v0.5.2 (2022-06-13)

 * [2f2d10ceb](https://github.com/numaproj/numaflow/commit/2f2d10cebf7158c83e1febb0b06e8e8e002a32cd) Update manifests to v0.5.2
 * [cedd0d1f8](https://github.com/numaproj/numaflow/commit/cedd0d1f8ef752fea1464799d90bff2fe009fe0d) feat: introduce source buffer and sink buffer (#53)
 * [d3301aa9f](https://github.com/numaproj/numaflow/commit/d3301aa9f0c3ae3771096422ec114686e7f7c21c) feat: async publishing for kafka sink (#51)
 * [2474eb8ec](https://github.com/numaproj/numaflow/commit/2474eb8ec3deaf132ee30e7881dddd3ac7460e18) fix spelling errors (#48)
 * [c4a12f87c](https://github.com/numaproj/numaflow/commit/c4a12f87c7aedb6f6ddda07e2d776a3a2c1b5c6a) feat: controller to create buckets (#47)
 * [eb97dc3bd](https://github.com/numaproj/numaflow/commit/eb97dc3bdc8e41d934c4ca17e7a97dcd192d3870) turn on watermark only if ENV value is true (#46)
 * [f189ba30e](https://github.com/numaproj/numaflow/commit/f189ba30e4e7f8beb5d4340c377e84574a2092cd) minimal end to end line-graph watermark integration (#43)

### Contributors

 * Derek Wang
 * Qianbo Huai
 * Vigith Maurice

## v0.5.1 (2022-06-02)

 * [bb9be807e](https://github.com/numaproj/numaflow/commit/bb9be807eddb68bf70a8e64e285d631ff3a1c4e0) Update manifests to v0.5.1
 * [912747eb0](https://github.com/numaproj/numaflow/commit/912747eb0cabac2a78950155bf5e37e7fe3a5e8b) Fixed spelling error (#44)
 * [3aeb33a87](https://github.com/numaproj/numaflow/commit/3aeb33a8709591b9cb0a14d55e3c44fd5f031437) Exponential buckets (#42)
 * [1d656829e](https://github.com/numaproj/numaflow/commit/1d656829e19f31111356d0e7a74d83c887a87dd0) fix: different behavior for time.After in go 1.18 (#39)

### Contributors

 * Derek Wang
 * Krithika3

