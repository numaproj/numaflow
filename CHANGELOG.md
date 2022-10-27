# Changelog

## v0.6.1 (2022-10-26)

 * [32b284f](https://github.com/numaproj/numaflow/commit/32b284f626aaffdfc16c267a3890e41cdc5f0142) Update manifests to v0.6.1
 * [9684e16](https://github.com/numaproj/numaflow/commit/9684e1616e97ae4eeb791d7a9164d2f31e9317a4) fix(manifests): Include ServiceAccount in namespace scoped install (#240)
 * [fe83918](https://github.com/numaproj/numaflow/commit/fe83918a458d632af71daccff56bf7d00aaaa012) fix(docs): fix a typo (#241)
 * [f2094b4](https://github.com/numaproj/numaflow/commit/f2094b4baa031f985068f5accf9426a599b72f97) feat: introduce reduce UDF. Closes #246 (#262)
 * [e19a1e7](https://github.com/numaproj/numaflow/commit/e19a1e7d90644378201eb8854b226b5c50c6cf9c) feat: add pandoc to required tools development doc. Closes #276 (#277)
 * [9a93711](https://github.com/numaproj/numaflow/commit/9a937118fd6ffcb5ff19803423019ede70e20d4b) feat: add isbsvc type and age to printcolumn. Closes #268 (#275)
 * [f25e303](https://github.com/numaproj/numaflow/commit/f25e303e773d4a0e1f3d32815f839628e35278f0) fix: watermark consumer fix (#273)
 * [d2a3d90](https://github.com/numaproj/numaflow/commit/d2a3d90823046b103b9729b142903ba9f1903bf4) refactor: generalize watermark fetching as an interface of ISB service. Fixes #252 (#263)
 * [5ffcadc](https://github.com/numaproj/numaflow/commit/5ffcadccafeef5711f784acb005c51051c06fd18) fix: set default property values for minimal CRD installation (#264)
 * [17a9956](https://github.com/numaproj/numaflow/commit/17a99564587bf4cc68d057bccd808ea611b1bf7d) fix: validate only one isbsvc implementation type is defined. Fixes #269 (#271)
 * [2272a1f](https://github.com/numaproj/numaflow/commit/2272a1fcbb8c0fdb676093eba1b1e27e13fef257) fix: main branch make build failure: math.MaxInt64 for int type (#265)
 * [02c31d2](https://github.com/numaproj/numaflow/commit/02c31d277b0a74f6e97aefd88c2e11f32d7b4f95) fix: nil pointer deref when running example with minimal CRD. Fixes #260 (#261)
 * [391b53e](https://github.com/numaproj/numaflow/commit/391b53e1203d0989f71bcad4840446e0dda55324) fix: retry only the failed offsets (#255)
 * [7b42dc8](https://github.com/numaproj/numaflow/commit/7b42dc80c813c694ba494cb6f6b86347745e5b7b) fix: re-enable build constraint on processor manager test. Fixes #256 (#257)
 * [3436049](https://github.com/numaproj/numaflow/commit/3436049011b6ed245b048ec81a9185cde1e48e62) fix: container resource for jetstream isbsvc. Fixes #253 (#254)
 * [33ce742](https://github.com/numaproj/numaflow/commit/33ce74222a76dc8deeb1306da2165d90571fdba1) fix: update vertex watermark fetching logic. Fixes: #134 (#245)
 * [fd219a5](https://github.com/numaproj/numaflow/commit/fd219a5cda3623549abf47e064a4549470056b59) fix: watermark watcher leak (#242)
 * [979a3a3](https://github.com/numaproj/numaflow/commit/979a3a3f8d0649cb6ac82722513a3c96827bfb70) feat: Support running UX server with namespace scope. Fixes #248 (#249)
 * [5e9d1c1](https://github.com/numaproj/numaflow/commit/5e9d1c1ceb88b3ad9cab2a947018e8935cbbd73f) fix: Watermark close fix and removed the nil check (#238)
 * [340bd82](https://github.com/numaproj/numaflow/commit/340bd820ddd25d507541dcd368ce1eaf51ecc9e3) fix: skip publishing watermarks to unexpected vertices. Fixes #235 (#236)
 * [904b2cd](https://github.com/numaproj/numaflow/commit/904b2cde562351fd39cc54b6ac0c91baa9ab3047) fix: update default watermark to -1. Fixes #133 (#218)
 * [321e285](https://github.com/numaproj/numaflow/commit/321e285fa437a0fbf33cb127ce6abbe1feaf0159) feat: support disabling TLS and changing port for UX server (#228)
 * [d0d74e1](https://github.com/numaproj/numaflow/commit/d0d74e19745084dbb677a56d04205ddff435d427) feat: reducer for stream aggregation without fault tolerance (#208)
 * [06a9b58](https://github.com/numaproj/numaflow/commit/06a9b58a3e3b8452e767f2cc34229b5ee0145aad) feat: in-memory watermark store for better testing (#216)
 * [f25cc58](https://github.com/numaproj/numaflow/commit/f25cc58e6b9fc504c3b6a15cbd503f479f60df1d) Add USERS.md (#221)
 * [a37cece](https://github.com/numaproj/numaflow/commit/a37cece931629f230ad9c981351641c23dfdb3f0) fix(watermark): generator should not publish wm for every message (#217)

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

 * [48aad5f](https://github.com/numaproj/numaflow/commit/48aad5fcbf855380b06f90c082e92571916e4c54) Update manifests to v0.6.0
 * [09ce54f](https://github.com/numaproj/numaflow/commit/09ce54f1008d5a822045b53779b1722aa503700f) fix(autoscaling): Ack pending should be included in total pending calculation (#212)
 * [9922787](https://github.com/numaproj/numaflow/commit/9922787ce1d00b97ac119081a303ae26d8281cb8) fix(autoscaling): Skip autoscaling if vertex is not in running phase (#207)
 * [bc2380a](https://github.com/numaproj/numaflow/commit/bc2380a7b6a035f14fbffe0a0cbfe613056e6b93) feat: ISBSVC add support for redis cluster mode (#195)
 * [72a96a5](https://github.com/numaproj/numaflow/commit/72a96a5843bc7dbcde4a092cbfc8e771d0e70bef) refactor: move controllers package to pkg/reconciler (#192)
 * [b1b78fa](https://github.com/numaproj/numaflow/commit/b1b78faafd0102f6c19d4905b60be4f4d97153ad) fix: update udf fetchWatermark and publishWatermark initial values (#193)
 * [d491260](https://github.com/numaproj/numaflow/commit/d491260060ae25d57b42b7df76781b34437cf355) fix(docs): readme for UI development (#181)
 * [6b121c6](https://github.com/numaproj/numaflow/commit/6b121c6e4f87acaea2a7828511c187f5508ea62a) feat: grpc udsink (#174)
 * [567da7b](https://github.com/numaproj/numaflow/commit/567da7b0d0235a171f7c7b3bdefb5b8b1ca5acb3) fix: numaflow-go udf example & docs (#177)
 * [4652f80](https://github.com/numaproj/numaflow/commit/4652f808a35b73c38f22d8d03df46405959198cd) fix: use scale.max if it is set (#179)
 * [900314b](https://github.com/numaproj/numaflow/commit/900314bc6c01e647b53c4fa916fd65dfd0ded221) fix broken path (#176)
 * [3b02f2a](https://github.com/numaproj/numaflow/commit/3b02f2a6a0e1a0e8bd03479eadb0f713a4de09fb) feat: Shuffle implementation (#169)
 * [021bb9d](https://github.com/numaproj/numaflow/commit/021bb9dfdb4a3d8da44275397f06d022b0edcfc4) feat: windowing operations (#157)
 * [7d41129](https://github.com/numaproj/numaflow/commit/7d411294a28cd9aa135efe3de14405ebc637e73c) feat: watermark for sources (#159)
 * [5f5b2df](https://github.com/numaproj/numaflow/commit/5f5b2dfdc4d999b65d4a3b7a5366e3234677bb61) fix: daemon service client memory leak (#161)
 * [bfe9669](https://github.com/numaproj/numaflow/commit/bfe966956cc9ea36759c62f5a787c0e1ed98fb68) pbq implementation (#155)
 * [8dfedd8](https://github.com/numaproj/numaflow/commit/8dfedd838794d2bee94b18f720057ef9e99b73e0) feat: check if udf is running in liveness probe  (#156)
 * [81e76d8](https://github.com/numaproj/numaflow/commit/81e76d82e375c4f5e9f392cf53a2624fc036878d) feat: Add udf grpc support Fixes #145  (#146)
 * [511faff](https://github.com/numaproj/numaflow/commit/511faffcb7ee6860d15d97dd54332a14300d88f8) refactor: some refactor on watermark (#149)
 * [7fe40c4](https://github.com/numaproj/numaflow/commit/7fe40c428c50af435b54be8d512cae40b6b7e49e) fix: Fixed JS bug (#144)
 * [24a16a0](https://github.com/numaproj/numaflow/commit/24a16a049f2f3a1752ee702f5020136a51d66e69) bug: watermark needs nil check
 * [f4ed831](https://github.com/numaproj/numaflow/commit/f4ed831ba0d0cbde92f1b7cc1113c83a0c77b702) fix: pipeline UI broken when vertex scaling down to 0 (#132)
 * [0ae0377](https://github.com/numaproj/numaflow/commit/0ae0377f6019d42dfd0625d86304299061cb18c8) feat: JetStream auto-reconnection (#127)
 * [2fc04eb](https://github.com/numaproj/numaflow/commit/2fc04eb3b87f42526c21c8648e7aa89a20c933f1) feat: Add watermark for sink vertex (#124)
 * [d958ee6](https://github.com/numaproj/numaflow/commit/d958ee6defda7e37dcf2192609634bb4d5f97be1) feat: autoscaling with back pressure factor (#123)
 * [b1f7768](https://github.com/numaproj/numaflow/commit/b1f776821e10231292fbc50dce1639fd492a61af) feat: add watermark to UI (#122)
 * [7feeaa8](https://github.com/numaproj/numaflow/commit/7feeaa87996dc173438b0259902266e66d05077b) feat: add processing rate to UI (#121)
 * [43fae93](https://github.com/numaproj/numaflow/commit/43fae931e947c90e3a15c62f65d9cacaf48bbcfa) feat: Expose watermark over HTTP (#120)
 * [ec02304](https://github.com/numaproj/numaflow/commit/ec02304a15e7436b0559f6443b2ab86d186067fe) fix: daemon service rest api not working (#119)
 * [f3da56d](https://github.com/numaproj/numaflow/commit/f3da56d36c23c9e356aa689d81348e7e21801d90) chore(deps): bump terser from 5.14.1 to 5.14.2 in /ui (#117)
 * [e2e63c8](https://github.com/numaproj/numaflow/commit/e2e63c84c77eb874629b5f547a2383da2f96e7d8) feat: Numaflow autoscaling (#115)
 * [e5da3f5](https://github.com/numaproj/numaflow/commit/e5da3f544e2f755d608c73d98c0aed108b813197) feat: watermark for headoffset (#116)
 * [a45b2ee](https://github.com/numaproj/numaflow/commit/a45b2eed124248a28460309a3ea472769c7562ef) feat: support namespace scope installation (#112)
 * [ce39199](https://github.com/numaproj/numaflow/commit/ce39199e76150fb1c88bfad35c92e57c23ea4b3a) feat: Expose ReadTimeoutSeconds on Vertex (#110)
 * [18ad1c5](https://github.com/numaproj/numaflow/commit/18ad1c5fbe2305632011e67d6e239cc8ab1f8f97) fix: imagepullpocily for local testing (#113)
 * [469849b](https://github.com/numaproj/numaflow/commit/469849b5b9c29889ee38f0712ad2267088bdda5c) feat: removed udfWorkers from limits and added some docs (#103)
 * [3fada66](https://github.com/numaproj/numaflow/commit/3fada667357ae9ead741ad24e7ba33b7cebcbf99) feat: Add icon and other minor changes (#94)
 * [a81838d](https://github.com/numaproj/numaflow/commit/a81838d7baa0f0b5001aa38cb2e6627bf9b2d977) feat: end to end tickgen watermark validation (#98)
 * [d7d9317](https://github.com/numaproj/numaflow/commit/d7d93175a93104e28e375d61e2dd33669764ef42) fix: Broken hyperlink (#96)
 * [a2e0792](https://github.com/numaproj/numaflow/commit/a2e079264dfc87b65018c57723119ccbe512c99a) add no-op KV Store (#91)
 * [45c8cb6](https://github.com/numaproj/numaflow/commit/45c8cb69dc9ebe2352f58e7cd71eb798bc542384) feat: no operation watermark progressor (#90)
 * [448c229](https://github.com/numaproj/numaflow/commit/448c229ab7d399505b616a65a9916cac00db3f4d) feat: kafka source pending messages (#80)
 * [1aa3930](https://github.com/numaproj/numaflow/commit/1aa39300ec8c46e262d9203dbaa4b6c4d72490ce) feat: Interface for Watermark (#82)
 * [be78c52](https://github.com/numaproj/numaflow/commit/be78c5237358fe6325a3a2609ea4597ea51257ab) feat: expose pending messages and processing rate (#79)
 * [df30f2a](https://github.com/numaproj/numaflow/commit/df30f2a84d13d535b226aaf0feb017ceb1952664) feat: Added the right way to decipher from and to vertex (#78)
 * [639c459](https://github.com/numaproj/numaflow/commit/639c459ac2ad92adabe618ad162d18dab45f5858) feat: define buffer limits on edges (#70)
 * [41fdd38](https://github.com/numaproj/numaflow/commit/41fdd38bd102ad91ad75e9d8a260a818762ec91d) feat: Merge UX server code (#67)
 * [ced9907](https://github.com/numaproj/numaflow/commit/ced9907908b3230f7f909341401d8d4934381240) feat: auto-scaling (part 1) (#59)
 * [fd5b374](https://github.com/numaproj/numaflow/commit/fd5b37412ab0cb66f2399f90b856b960e570e368) Added name to service spec (#58)
 * [dc2badf](https://github.com/numaproj/numaflow/commit/dc2badfdc046c214aef71f32a9cd3a60038bff41) feat: introduce source buffer and sink buffer (#53)
 * [4ed83a2](https://github.com/numaproj/numaflow/commit/4ed83a2aede73510da17aab5431c9e3e549a5d47) feat: async publishing for kafka sink (#51)
 * [9f9f5ba](https://github.com/numaproj/numaflow/commit/9f9f5ba73a4bcfb766085c349dffdde15ce32135) fix spelling errors (#48)
 * [f423002](https://github.com/numaproj/numaflow/commit/f423002efb8f307c995bbf59e63fb7bc52d85d31) feat: controller to create buckets (#47)
 * [8328739](https://github.com/numaproj/numaflow/commit/8328739c6534473a3892aeaedc4261b43449efc4) turn on watermark only if ENV value is true (#46)
 * [46f72e2](https://github.com/numaproj/numaflow/commit/46f72e237d5a153be6c59bd736130fe70abaf1e0) minimal end to end line-graph watermark integration (#43)
 * [1f8203f](https://github.com/numaproj/numaflow/commit/1f8203f4ad13ae4e9713373d7858f0096957c93e) Fixed spelling error (#44)
 * [f1e99ea](https://github.com/numaproj/numaflow/commit/f1e99eae3b9e5a49cc651f8ef47a912329549960) Exponential buckets (#42)
 * [dfcfdeb](https://github.com/numaproj/numaflow/commit/dfcfdeba8b2c86661147ef53707aa2d3f46c5074) fix: different behavior for time.After in go 1.18 (#39)

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

 * [ac15d22](https://github.com/numaproj/numaflow/commit/ac15d229af7ba127c162815d83508bb62b6b35b5) Update manifests to v0.5.6
 * [f236375](https://github.com/numaproj/numaflow/commit/f2363757fca2b51cc466afe344fb54215c4c5051) feat: grpc udsink (#174)
 * [2650c2d](https://github.com/numaproj/numaflow/commit/2650c2de59f1903a269dd3c15af0e0c285e5d290) fix: numaflow-go udf example & docs (#177)
 * [c44f733](https://github.com/numaproj/numaflow/commit/c44f733f2b20c1e2d75664f4b71066b35ea6bc3b) fix: use scale.max if it is set (#179)
 * [39e92d0](https://github.com/numaproj/numaflow/commit/39e92d06380cba339f692251d7ca319b5fc481cb) fix broken path (#176)
 * [46ce0f8](https://github.com/numaproj/numaflow/commit/46ce0f879758b38153d7d4a58c00e8714ce8871d) feat: Shuffle implementation (#169)
 * [71ca00a](https://github.com/numaproj/numaflow/commit/71ca00a1b425706d7ef7bbc6f5f19ff8f2718305) feat: windowing operations (#157)
 * [ca00b78](https://github.com/numaproj/numaflow/commit/ca00b78f2d8ece0bfee426ea2f2b2216fa24f127) feat: watermark for sources (#159)
 * [91e21ce](https://github.com/numaproj/numaflow/commit/91e21ceec78ced5ad53979c36b70be39198a5af5) pbq implementation (#155)
 * [6542400](https://github.com/numaproj/numaflow/commit/654240042a1d302166fdbc1ac367c92bc052b19a) feat: check if udf is running in liveness probe  (#156)
 * [79dce0b](https://github.com/numaproj/numaflow/commit/79dce0b386d7fec7a008a1d0a1e4d7bf9835ecaa) feat: Add udf grpc support Fixes #145  (#146)

### Contributors

 * Chrome
 * Derek Wang
 * Juanlu Yu
 * Vigith Maurice
 * Yashash H L

## v0.5.5 (2022-09-07)

 * [9aae638](https://github.com/numaproj/numaflow/commit/9aae638c4bc0011e027b40c4b7a3b17b189ea945) Update manifests to v0.5.5
 * [3241432](https://github.com/numaproj/numaflow/commit/324143252c3657b85c643b074949679dcd4f26ee) fix: daemon service client memory leak (#161)
 * [be47a26](https://github.com/numaproj/numaflow/commit/be47a26af5965897951da35a53eff6d5f423df89) refactor: some refactor on watermark (#149)
 * [857cce7](https://github.com/numaproj/numaflow/commit/857cce75b86d7aa96cdeebbf822bfb52607e05da) fix: Fixed JS bug (#144)
 * [da16abc](https://github.com/numaproj/numaflow/commit/da16abc7e623d0e25564fdb64cc6c3f01c23e88d) bug: watermark needs nil check
 * [c9998a1](https://github.com/numaproj/numaflow/commit/c9998a1ca3926f37d2180e1082beacdb24d0b3b1) fix: pipeline UI broken when vertex scaling down to 0 (#132)

### Contributors

 * Derek Wang
 * Krithika3
 * Vigith Maurice

## v0.5.4 (2022-08-05)

 * [57513b4](https://github.com/numaproj/numaflow/commit/57513b408eddd8e7918cab540ea866ad19d13518) Update manifests to v0.5.4
 * [94cdb82](https://github.com/numaproj/numaflow/commit/94cdb82febe92021a37ea44c466949982da13910) feat: JetStream auto-reconnection (#127)
 * [8d83540](https://github.com/numaproj/numaflow/commit/8d8354082966a524275539dfc2c31e2c2a2c47bc) feat: Add watermark for sink vertex (#124)
 * [228ba32](https://github.com/numaproj/numaflow/commit/228ba3216bdfa1b667407be8310e5561f5fea90e) feat: autoscaling with back pressure factor (#123)
 * [9833efd](https://github.com/numaproj/numaflow/commit/9833efdf14892f20ea792a042d03adec4ad3a91a) feat: add watermark to UI (#122)
 * [0dab55d](https://github.com/numaproj/numaflow/commit/0dab55d8707a7172b37e4e59053ea0d770520982) feat: add processing rate to UI (#121)
 * [ffd38a1](https://github.com/numaproj/numaflow/commit/ffd38a1528214f2a09986459f3a14588276fbbe0) feat: Expose watermark over HTTP (#120)
 * [c09502a](https://github.com/numaproj/numaflow/commit/c09502a286b8f35ede0ccc43545afc391d967e58) fix: daemon service rest api not working (#119)
 * [ebc10f4](https://github.com/numaproj/numaflow/commit/ebc10f4117d5ee19dbe6ad4915b7f63f14325373) chore(deps): bump terser from 5.14.1 to 5.14.2 in /ui (#117)
 * [84490ca](https://github.com/numaproj/numaflow/commit/84490ca852f95f661fbe93687180672ad5ecacca) feat: Numaflow autoscaling (#115)
 * [32b9848](https://github.com/numaproj/numaflow/commit/32b98486f4c7f004ed4b36be5f4af18e29d71969) feat: watermark for headoffset (#116)
 * [283dae9](https://github.com/numaproj/numaflow/commit/283dae9073f59de35b82ba3b4d243204d9d77067) feat: support namespace scope installation (#112)
 * [8e612b1](https://github.com/numaproj/numaflow/commit/8e612b1fa2b3ca3c0ad037ab816b7ddc1322dd7d) feat: Expose ReadTimeoutSeconds on Vertex (#110)
 * [d95d41b](https://github.com/numaproj/numaflow/commit/d95d41bd6446cd0b1312b93da4a88dd305b29ce4) fix: imagepullpocily for local testing (#113)

### Contributors

 * Derek Wang
 * Krithika3
 * Saravanan Balasubramanian
 * Sidhant Kohli
 * Vigith Maurice
 * dependabot[bot]

## v0.5.3 (2022-07-08)

 * [efee544](https://github.com/numaproj/numaflow/commit/efee5442c7618959319a1825f467f059fe67ac57) Update manifests to v0.5.3
 * [5895fac](https://github.com/numaproj/numaflow/commit/5895facd75b3fe2ba296a7283bca61e9b7b9e4e5) feat: removed udfWorkers from limits and added some docs (#103)
 * [0b75495](https://github.com/numaproj/numaflow/commit/0b75495f1b5795de619cb19430d2b125457e119a) feat: Add icon and other minor changes (#94)
 * [7eb08f5](https://github.com/numaproj/numaflow/commit/7eb08f58b7730357ed5c827c45ceda6177f5cc37) feat: end to end tickgen watermark validation (#98)
 * [3338e65](https://github.com/numaproj/numaflow/commit/3338e6589c7910b30813e9a9912916956f1d3a7e) fix: Broken hyperlink (#96)
 * [e311222](https://github.com/numaproj/numaflow/commit/e311222937098b49d17f2167f9002adefa1e2461) add no-op KV Store (#91)
 * [5d2f90e](https://github.com/numaproj/numaflow/commit/5d2f90ed0af7adbd2e2ddffd96b71577ce78e604) feat: no operation watermark progressor (#90)
 * [f58d0f4](https://github.com/numaproj/numaflow/commit/f58d0f4989a1569e45a6b66056e99db46f2b3218) feat: kafka source pending messages (#80)
 * [cbb16ca](https://github.com/numaproj/numaflow/commit/cbb16ca23db1054c8870b279fad47c183e5ad76a) feat: Interface for Watermark (#82)
 * [5592bb1](https://github.com/numaproj/numaflow/commit/5592bb1b453bf00be2c756487614700820a6c95f) feat: expose pending messages and processing rate (#79)
 * [06a3df2](https://github.com/numaproj/numaflow/commit/06a3df2d310f8d52bcb062c7d3e6a249723796d5) feat: Added the right way to decipher from and to vertex (#78)
 * [a0908ad](https://github.com/numaproj/numaflow/commit/a0908ad49a356b7f1cd40d40e5e8efd7f1994205) feat: define buffer limits on edges (#70)
 * [a1d3639](https://github.com/numaproj/numaflow/commit/a1d363955b4c4a2ff9bddb821e43a955e370fc4c) feat: Merge UX server code (#67)
 * [571c48e](https://github.com/numaproj/numaflow/commit/571c48eb039bc8b4e27e87b8f959aa2d72f56f23) feat: auto-scaling (part 1) (#59)
 * [1e0384b](https://github.com/numaproj/numaflow/commit/1e0384ba76994b68cb75a0967cb1e0460bc19b75) Added name to service spec (#58)

### Contributors

 * Derek Wang
 * Krithika3
 * Sidhant Kohli
 * Vigith Maurice

## v0.5.2 (2022-06-13)

 * [2f2d10c](https://github.com/numaproj/numaflow/commit/2f2d10cebf7158c83e1febb0b06e8e8e002a32cd) Update manifests to v0.5.2
 * [cedd0d1](https://github.com/numaproj/numaflow/commit/cedd0d1f8ef752fea1464799d90bff2fe009fe0d) feat: introduce source buffer and sink buffer (#53)
 * [d3301aa](https://github.com/numaproj/numaflow/commit/d3301aa9f0c3ae3771096422ec114686e7f7c21c) feat: async publishing for kafka sink (#51)
 * [2474eb8](https://github.com/numaproj/numaflow/commit/2474eb8ec3deaf132ee30e7881dddd3ac7460e18) fix spelling errors (#48)
 * [c4a12f8](https://github.com/numaproj/numaflow/commit/c4a12f87c7aedb6f6ddda07e2d776a3a2c1b5c6a) feat: controller to create buckets (#47)
 * [eb97dc3](https://github.com/numaproj/numaflow/commit/eb97dc3bdc8e41d934c4ca17e7a97dcd192d3870) turn on watermark only if ENV value is true (#46)
 * [f189ba3](https://github.com/numaproj/numaflow/commit/f189ba30e4e7f8beb5d4340c377e84574a2092cd) minimal end to end line-graph watermark integration (#43)

### Contributors

 * Derek Wang
 * Qianbo Huai
 * Vigith Maurice

## v0.5.1 (2022-06-02)

 * [bb9be80](https://github.com/numaproj/numaflow/commit/bb9be807eddb68bf70a8e64e285d631ff3a1c4e0) Update manifests to v0.5.1
 * [912747e](https://github.com/numaproj/numaflow/commit/912747eb0cabac2a78950155bf5e37e7fe3a5e8b) Fixed spelling error (#44)
 * [3aeb33a](https://github.com/numaproj/numaflow/commit/3aeb33a8709591b9cb0a14d55e3c44fd5f031437) Exponential buckets (#42)
 * [1d65682](https://github.com/numaproj/numaflow/commit/1d656829e19f31111356d0e7a74d83c887a87dd0) fix: different behavior for time.After in go 1.18 (#39)

### Contributors

 * Derek Wang
 * Krithika3

