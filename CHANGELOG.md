# Changelog

## v1.1.5 (2024-01-23)

 * [e5bcf32e](https://github.com/numaproj/numaflow/commit/e5bcf32e345454c577ecdd8a17e48e44d546730b) Update manifests to v1.1.5
 * [266cb227](https://github.com/numaproj/numaflow/commit/266cb2276d29c3ddbfd5cdb5ce16f9a50da62042) fix(controller): incorrect cpu/mem resources calculation (#1477)

### Contributors

 * Derek Wang

## v1.1.4 (2024-01-20)

 * [7ffb521b](https://github.com/numaproj/numaflow/commit/7ffb521bcc15612d04fe66de33d199e8c8391a7a) Update manifests to v1.1.4
 * [de780b95](https://github.com/numaproj/numaflow/commit/de780b95da57437ceb4fc5d7bc77619c7e9deb2d) fix: bug in late message handling for sliding window (#1472)

### Contributors

 * Derek Wang
 * Yashash H L

## v1.1.3 (2024-01-14)

 * [0b96acf9](https://github.com/numaproj/numaflow/commit/0b96acf9ba9b478e7284e6b5724822ac26a09ba8) Update manifests to v1.1.3
 * [907949be](https://github.com/numaproj/numaflow/commit/907949be3da2295403d2367806c317be13452e68) fix: GetDownstreamEdges is not cycle safe (#1447)
 * [1d83b51e](https://github.com/numaproj/numaflow/commit/1d83b51e8a9845a3b23e5ade83df2df599b0b9e4) chore(deps): bump follow-redirects from 1.15.3 to 1.15.4 in /ui (#1448)
 * [855672dd](https://github.com/numaproj/numaflow/commit/855672ddb483931650bfc46e3075fe6d3a5c7998) fix: UI Filter by status for pipelines doesn't work as expected (#1444)
 * [c06de95e](https://github.com/numaproj/numaflow/commit/c06de95eb56ce7c656496bfed5930a6c19db7555) fix: Kafka source reads duplicated messages (#1438)
 * [17c9c0e2](https://github.com/numaproj/numaflow/commit/17c9c0e2e5dc4803bb2d1d977a441500bb5b771d) feat: enhance autoscaling peeking logic (#1432)

### Contributors

 * Derek Wang
 * Juanlu Yu
 * Nishchith Shetty
 * akash khamkar
 * dependabot[bot]

## v1.1.2 (2024-01-01)

 * [ac716ec4](https://github.com/numaproj/numaflow/commit/ac716ec4ab4b49f4f013f067c33d1d89936e132a) Update manifests to v1.1.2
 * [af17d8ce](https://github.com/numaproj/numaflow/commit/af17d8ce25665ddfe8e6eb65ed97c0a743eae4f4) fix: server-secrets-init container restart  (#1433)

### Contributors

 * Derek Wang
 * Vedant Gupta

## v1.1.1 (2023-12-21)

 * [5ff77fe0](https://github.com/numaproj/numaflow/commit/5ff77fe0d6532ea5a513b7b94e6dea2af883ab2b) Update manifests to v1.1.1
 * [5fd20ad9](https://github.com/numaproj/numaflow/commit/5fd20ad9a5c2a6d949e510633eb00887a3fa44da) chore(deps): bump golang.org/x/crypto from 0.14.0 to 0.17.0 (#1424)
 * [da32632c](https://github.com/numaproj/numaflow/commit/da32632c9a0cb47e2b04d046471dddc3befb8766) fix: configmap const name (#1423)

### Contributors

 * Derek Wang
 * dependabot[bot]

## v1.1.0 (2023-12-18)

 * [07d46ca9](https://github.com/numaproj/numaflow/commit/07d46ca9d0358db3625120328b428274ace54f2f) Update manifests to v1.1.0
 * [41b8dffc](https://github.com/numaproj/numaflow/commit/41b8dffc46a5c46e8d8412c69ef291a8be3821da) feat: local user support for Numaflow (#1416)
 * [818be4f2](https://github.com/numaproj/numaflow/commit/818be4f20959ec8c9dd177039fe327ae6a22fb1f) fix: consider lastPublishedIdleWm when computed watermark is -1 (#1415)
 * [263263b3](https://github.com/numaproj/numaflow/commit/263263b30eee79a62588f791da0ce4295ff07877) fix: access path for auth endpoints (#1403)
 * [0db9cd19](https://github.com/numaproj/numaflow/commit/0db9cd191ebb9406aa5dc94a0766597bc3f03f03) feat: Generate Idle Watermark if the source is idling (#1385)
 * [6eb25c25](https://github.com/numaproj/numaflow/commit/6eb25c251263eb0d452100968ff9f8cf9b52382b) fix: include dropped messages in source watermark calculation (#1404)
 * [1eee1942](https://github.com/numaproj/numaflow/commit/1eee1942d836c9cfd42417867b3126ea6eaebbf3) chore(deps): bump @adobe/css-tools from 4.3.1 to 4.3.2 in /ui (#1400)
 * [0a2ff566](https://github.com/numaproj/numaflow/commit/0a2ff5663d6b15f9e0ec4c40d916003ed051f40e) fix: updated access path config for root path (#1397)
 * [024597d5](https://github.com/numaproj/numaflow/commit/024597d5bb95144a48c91af1dd4a3c89173b4808) feat: improve numaflow k8s events (#1393)
 * [83fb9068](https://github.com/numaproj/numaflow/commit/83fb9068190e4f2ce8e09d8a41212d4848e597d6) fix: update numaflow-go version (#1387)
 * [9b0fadb6](https://github.com/numaproj/numaflow/commit/9b0fadb6e4746e6acb232b2637c74dca60536c78) fix: access path for api/v1 route (#1388)
 * [96cfa555](https://github.com/numaproj/numaflow/commit/96cfa55522ad8c7cfc4d8ddeac2e68ab964e919c) fix: dropped messages should not be considered for watermark propagation (#1386)
 * [5bf63076](https://github.com/numaproj/numaflow/commit/5bf63076c45b0e95bd363d80855c4a880509b9f2) chore(deps): bump github.com/go-jose/go-jose/v3 from 3.0.0 to 3.0.1 (#1383)
 * [0c82ee0f](https://github.com/numaproj/numaflow/commit/0c82ee0fd8ed296ba0750eefc554fcb2e1606d4a) refactor: move udf forwarder to the right dir (#1381)
 * [3a77ed42](https://github.com/numaproj/numaflow/commit/3a77ed42d1ce48e41f0b3cca81cd9c940a1fc1c0) fix: add pipeline update validation checks (#1379)
 * [44a38f4a](https://github.com/numaproj/numaflow/commit/44a38f4a9f411417d9abaa2aef82a9e9492edfd0) fix: disallow updating an existing isbsvc's persistence strategy (#1376)
 * [82538b6a](https://github.com/numaproj/numaflow/commit/82538b6a65f280599ed59a2b0bcbfe0b7999ea1a) fix: add more checks to isbsvc validation (#1358)
 * [118c309d](https://github.com/numaproj/numaflow/commit/118c309d27f84f9306933a4147a86f03cd9c65ef) fix: non-ack failed offsets (#1370)
 * [3456f714](https://github.com/numaproj/numaflow/commit/3456f7147c8f512caad320e895f9ce0c23cbf741) feat: validate patched data for pipelines (#1349)
 * [e75e9581](https://github.com/numaproj/numaflow/commit/e75e958116550550f3a3bf8cadbdadcef1cc07f0) Add Atlan into USERS.md (#1351)
 * [483c0185](https://github.com/numaproj/numaflow/commit/483c01855be75a3dd903d351fe7a129c60f8b135) Unit tests UI (#1348)
 * [d0ae1484](https://github.com/numaproj/numaflow/commit/d0ae1484329e3d68fac1884e06dc39564c48367c) fix(SERVER): remove unknown filter (#1346)
 * [7ac77521](https://github.com/numaproj/numaflow/commit/7ac77521e097fe897377a564da3a7062061976dc) Upstreammain (#1345)
 * [f1af1f02](https://github.com/numaproj/numaflow/commit/f1af1f02c31ecc8950d61d9460bbd0d496ead01d) fix: rc-4 bug bash bug fixes (#1343)
 * [f6edd5ae](https://github.com/numaproj/numaflow/commit/f6edd5ae53099ce174ab643ebd0608b447ddc399) fix(UI): rc-0.4 fixes (#1342)
 * [ecbd489f](https://github.com/numaproj/numaflow/commit/ecbd489f87364caf50482f340cb2325d80f7960c) fix(SERVER): fix styles for ISB cards
 * [d4836e9e](https://github.com/numaproj/numaflow/commit/d4836e9e3796c02e1e6ec30a6944febec12d1db1) fix(SERVER): fix styles for ISB cards
 * [953feb67](https://github.com/numaproj/numaflow/commit/953feb67991b1debceb8b69a85648434c49c882c) fix(SERVER): fix for pagination issue
 * [d484393d](https://github.com/numaproj/numaflow/commit/d484393d92a489e61763978f1a1c754d5087fffb) fix(SERVER): namespace inout filtering space alignment
 * [e9360127](https://github.com/numaproj/numaflow/commit/e93601275981dc6129116f6086d104c4f78746ae) fix(SERVER): pipeline card style fix
 * [c24b91ce](https://github.com/numaproj/numaflow/commit/c24b91cead6dc094fca3764e6211860c3d9a668e) fix: block pipeline load post update (#1333)
 * [e928be34](https://github.com/numaproj/numaflow/commit/e928be34de04d62b6ef6633b5593fd4efab3456f) fix: full isb spec in edit (#1331)
 * [d5c3b07a](https://github.com/numaproj/numaflow/commit/d5c3b07a050b25a9a376e69e4be603fbdeb37dc8) RC2.0 UI fixes (#1329)
 * [f4211fb1](https://github.com/numaproj/numaflow/commit/f4211fb1e1b40ad2bd47cc119e2bfdebbb747250) feat: container for generator vertices (#1321)
 * [f4354af3](https://github.com/numaproj/numaflow/commit/f4354af308b4de4e797609d45374b5a4bdd0d59e) fix: create isb should move to isb tab (#1323)
 * [24afc5e1](https://github.com/numaproj/numaflow/commit/24afc5e12a06c739f9ccd453e90a730f4049550b) fix: max lag (#1319)
 * [42f81df4](https://github.com/numaproj/numaflow/commit/42f81df44e4f80e24b72199b625e616acb06583e) feat: cache daemon client for each pipeline (#1276)
 * [a7270426](https://github.com/numaproj/numaflow/commit/a72704266b2634439e4929f8bb5dc54cf0262cde) fix: rc2 UI fixes (#1317)
 * [01bf1854](https://github.com/numaproj/numaflow/commit/01bf18544824e8b42291be64c4a16123ae54c752) feat: added filtering based on number, status and health of pipelines… (#1312)
 * [91b0effa](https://github.com/numaproj/numaflow/commit/91b0effa1275d86c616ff3076bd35b75fe3a0660) fix: user identity cookie max age (#1316)
 * [5c999b63](https://github.com/numaproj/numaflow/commit/5c999b631f030402c2239daaf98bb68427747632) feat: add scopes to authorization (#1288)
 * [e119a0ee](https://github.com/numaproj/numaflow/commit/e119a0ee666c8804c9296a3f61639ec181a86bf1) fix: logout fix (#1310)
 * [449dfd3a](https://github.com/numaproj/numaflow/commit/449dfd3a3c085dea8d3ac3a6d1c8ba4a67c92fba) fix: Split cookie to meet the cookie length requirement (#1305)
 * [87c4c1e1](https://github.com/numaproj/numaflow/commit/87c4c1e16e5da8e583a14a97be25696652c72cb5) chore(deps): bump github.com/nats-io/nats-server/v2 from 2.10.3 to 2.10.4 (#1307)
 * [ea577452](https://github.com/numaproj/numaflow/commit/ea5774527f9cd34868b379805eca0143acc0de7d) add tooltips (#1289)
 * [93eec963](https://github.com/numaproj/numaflow/commit/93eec9639df4aa0f15fd7ce7e1588efc75a128b4) fix: fixed the timer not clearing issue (#1303)
 * [c7bdbda5](https://github.com/numaproj/numaflow/commit/c7bdbda569a264b618c468743af99ced9828780f) feat: k8s events filtering and cluster summary card fixes  (#1297)
 * [a558b229](https://github.com/numaproj/numaflow/commit/a558b229d499ba6bffecefaa57c591428ef7c0b4) fix: graph overflow with large height (#1301)
 * [73ffaa86](https://github.com/numaproj/numaflow/commit/73ffaa86e319da838fca12aa392f6b2ff171b9f2) doc: need metrics server (#1296)
 * [32416bf2](https://github.com/numaproj/numaflow/commit/32416bf263c4661086ae7d6f9046d2f31b137eaf) refactor: unified metrics names for forwarders (#1290)
 * [17b7b313](https://github.com/numaproj/numaflow/commit/17b7b313fbed721c7ce3cb080c70e05a8d5675bd) feat: added separate colors for sideInput and dynamic legend (#1292)
 * [6efab64e](https://github.com/numaproj/numaflow/commit/6efab64ef7f16936b470f39a43b7d67e6e188c20) feat: add tabs to display pipelines and isb services (#1293)
 * [86df4a84](https://github.com/numaproj/numaflow/commit/86df4a84083613cf3912f35ada400434ce1b200c) fix: more sidebar testing (#1287)
 * [d7ae1d36](https://github.com/numaproj/numaflow/commit/d7ae1d36ad600b08d18632964ae5d8aea5c01504) refactor: create interfaces for AuthN and AuthZ (#1286)
 * [65aca23f](https://github.com/numaproj/numaflow/commit/65aca23f6192a451033cfe25dff820dfb2102ae4) fix: tests for utils (#1283)

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

 * [78134e8f](https://github.com/numaproj/numaflow/commit/78134e8f0396cb5d4acb7fe6d8cbcd2768f80f5a) Update manifests to v1.0.0
 * [660ff501](https://github.com/numaproj/numaflow/commit/660ff5010b2191de4c015bccc7a4ad4be1e81388) fix: rc-4 bug bash bug fixes (#1343)
 * [ceed5def](https://github.com/numaproj/numaflow/commit/ceed5def03c4d1d9c011b4839b2606a7741a39f3) fix(UI): rc-0.4 fixes (#1342)

### Contributors

 * Darshan Simha
 * Derek Wang
 * mshakira

## v1.0.0-rc4 (2023-11-03)

 * [e94b563f](https://github.com/numaproj/numaflow/commit/e94b563f44a0469fc88bed0351d14e7587c0fb61) Update manifests to v1.0.0-rc4
 * [14757afc](https://github.com/numaproj/numaflow/commit/14757afcc6d76603e8229357671d2092bab30f33) fix(SERVER): fix styles for ISB cards
 * [fc90920b](https://github.com/numaproj/numaflow/commit/fc90920b93315830d24d0e16c5754823b9f7b3b7) fix(SERVER): fix styles for ISB cards
 * [8bf275bd](https://github.com/numaproj/numaflow/commit/8bf275bd33c465276a1b714c4c2f5a7024f96c42) fix(SERVER): fix for pagination issue
 * [2e8cff7a](https://github.com/numaproj/numaflow/commit/2e8cff7acb824ec76d07876651717c297407929e) fix(SERVER): namespace inout filtering space alignment
 * [c77fb0fc](https://github.com/numaproj/numaflow/commit/c77fb0fcf66397642271b20bd86f9732110f8d19) fix(SERVER): pipeline card style fix
 * [52d9370f](https://github.com/numaproj/numaflow/commit/52d9370f585f7984834dbd11c257c9fefb9b137f) fix: block pipeline load post update (#1333)
 * [2307660f](https://github.com/numaproj/numaflow/commit/2307660f731a370929811dd584b3ad16383c3e1f) fix: full isb spec in edit (#1331)
 * [a70c77b9](https://github.com/numaproj/numaflow/commit/a70c77b93b4b8e7d5688a7cbabbba35392b1ea80) RC2.0 UI fixes (#1329)
 * [02281f38](https://github.com/numaproj/numaflow/commit/02281f38fdbe84f6d28ad2414a47636135f093f5) feat: container for generator vertices (#1321)
 * [feb4977a](https://github.com/numaproj/numaflow/commit/feb4977a9e806ec1dcf4b79e04f2991338df60c9) fix: create isb should move to isb tab (#1323)
 * [e05132ef](https://github.com/numaproj/numaflow/commit/e05132ef9b2a9d9d2ffd500a9c8ddc5c5bee9b2f) fix: max lag (#1319)

### Contributors

 * Bradley Behnke
 * Darshan Simha
 * Derek Wang
 * Juanlu Yu
 * Shakira M
 * Vedant Gupta
 * mshakira

## v1.0.0-rc3 (2023-11-01)

 * [6ab96b18](https://github.com/numaproj/numaflow/commit/6ab96b1823a1de438cb1fb6a3733282fe4468fc1) Update manifests to v1.0.0-rc3
 * [4ffda383](https://github.com/numaproj/numaflow/commit/4ffda3834fae7c2487cafd61855f6da520b07276) feat: cache daemon client for each pipeline (#1276)
 * [5a7d739e](https://github.com/numaproj/numaflow/commit/5a7d739e233c0632aa8b36628687cd35f5a449d4) fix: rc2 UI fixes (#1317)
 * [cbd810bc](https://github.com/numaproj/numaflow/commit/cbd810bc4b1835d55de0d4641313d44ec79ab2fe) feat: added filtering based on number, status and health of pipelines… (#1312)
 * [491c8786](https://github.com/numaproj/numaflow/commit/491c87867a05eaaac2e0f6cc4526aa490cfdc81b) fix: user identity cookie max age (#1316)

### Contributors

 * Darshan Simha
 * Derek Wang
 * Dillen Padhiar
 * Juanlu Yu
 * mshakira

## v1.0.0-rc2 (2023-11-01)

 * [8a7dc592](https://github.com/numaproj/numaflow/commit/8a7dc5920c143aeaca06ff9c5cfc633102bac7d6) Update manifests to v1.0.0-rc2
 * [8ed52b39](https://github.com/numaproj/numaflow/commit/8ed52b39c086532bc5b8e1a53352718219c718c6) feat: add scopes to authorization (#1288)
 * [eebe623f](https://github.com/numaproj/numaflow/commit/eebe623f8464e37dc2e5674966917c142bb92441) fix: logout fix (#1310)
 * [9c34c27f](https://github.com/numaproj/numaflow/commit/9c34c27fb1a16e78bdbf1be16ad321fd71f1801b) fix: Split cookie to meet the cookie length requirement (#1305)
 * [c6073b17](https://github.com/numaproj/numaflow/commit/c6073b17c7e664095549774f6b578e205b86a1a4) chore(deps): bump github.com/nats-io/nats-server/v2 from 2.10.3 to 2.10.4 (#1307)
 * [b1bb6575](https://github.com/numaproj/numaflow/commit/b1bb65751aad08bbf3ff697f4f102ecbdbb09d92) add tooltips (#1289)
 * [40ca1705](https://github.com/numaproj/numaflow/commit/40ca1705b6086c5683d67f8d5adc1805258c291e) fix: fixed the timer not clearing issue (#1303)
 * [2a91668d](https://github.com/numaproj/numaflow/commit/2a91668d021c5346daab2188ba5353c84b3e9815) feat: k8s events filtering and cluster summary card fixes  (#1297)
 * [4929a1e4](https://github.com/numaproj/numaflow/commit/4929a1e4543549f8698d3d1510364353662d9816) fix: graph overflow with large height (#1301)
 * [8cc04a77](https://github.com/numaproj/numaflow/commit/8cc04a77a0c12bc550f3783cb31274493094e996) doc: need metrics server (#1296)
 * [05b19400](https://github.com/numaproj/numaflow/commit/05b19400509cfeead2c96f52c6d33a243257c507) refactor: unified metrics names for forwarders (#1290)
 * [5475e682](https://github.com/numaproj/numaflow/commit/5475e68205fe8899ae2f93bd73281e3e109e3e48) feat: added separate colors for sideInput and dynamic legend (#1292)
 * [052201e0](https://github.com/numaproj/numaflow/commit/052201e0b803e0a57d562056d28d9b3b605728da) feat: add tabs to display pipelines and isb services (#1293)
 * [9e1ea4f0](https://github.com/numaproj/numaflow/commit/9e1ea4f07bbb1642f3d670c9c2398b58f262be06) fix: more sidebar testing (#1287)
 * [d018af50](https://github.com/numaproj/numaflow/commit/d018af50bec64c1012a27d51fd407a548a364118) refactor: create interfaces for AuthN and AuthZ (#1286)
 * [ada5ea4d](https://github.com/numaproj/numaflow/commit/ada5ea4d73c0ec11852328104d2cd386b288a81b) fix: tests for utils (#1283)

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

 * [0ff1f58f](https://github.com/numaproj/numaflow/commit/0ff1f58fa371795a321c414c94a40c6b08ca32fe) Update manifests to v1.0.0-rc1
 * [16c3fc3c](https://github.com/numaproj/numaflow/commit/16c3fc3c55911688f0478b2d3966d0f6229d59e1) fix: incorrect image version for  namespaced numaflow-server (#1282)
 * [18d62935](https://github.com/numaproj/numaflow/commit/18d6293589a56ae7d7fc398871eec95564d0c41f) fix: ISBCreate test and fetch mock setup (#1279)
 * [9325140c](https://github.com/numaproj/numaflow/commit/9325140c81e9dee50c407ab93fb1fd4510d95992) fix: update dex to work with basehref (#1278)
 * [36610a5a](https://github.com/numaproj/numaflow/commit/36610a5ac107815460b1ba1dbb375d253925a372) feat: AuthN/AuthZ for Numaflow UI (#1234)
 * [730552e8](https://github.com/numaproj/numaflow/commit/730552e8dc3acdcfc8607dc3f2875459ee9cc373) fix(doc): hpa api version (#1274)
 * [c103427b](https://github.com/numaproj/numaflow/commit/c103427b553bda96b103e57bb1cd0e44e9a72cd2) fix: updating example.md (#1262)
 * [c1725b18](https://github.com/numaproj/numaflow/commit/c1725b1848a5fba88eead794e61dff125f49d06b) Feat/side input tests (#1257)
 * [5554bd65](https://github.com/numaproj/numaflow/commit/5554bd6572f62be9a0cff1fa74ab8d6896fcf1a6) chore(deps): bump google.golang.org/grpc from 1.57.0 to 1.57.1 (#1268)
 * [b86b2254](https://github.com/numaproj/numaflow/commit/b86b22541272474b8985be41aa0efb5826a5fce2) fix(UI): pod selection fix (#1266)
 * [07a7b8ea](https://github.com/numaproj/numaflow/commit/07a7b8eace4b08cf33696f35aafed3754cd257e5) doc: Numaflow high level security (#1264)
 * [308acf2f](https://github.com/numaproj/numaflow/commit/308acf2f05df81e9c413025bcecb9645287e9546) fix: fixed the ns-summary page to allow creation of pipeline when no … (#1263)
 * [ce737732](https://github.com/numaproj/numaflow/commit/ce737732a2b6f6748514555e4299b7efee8b7250) Unit tests graph page (#1250)
 * [639a9364](https://github.com/numaproj/numaflow/commit/639a936438e0ee8312d5c0534c5c3ddd46ecd24c) feat: Add e2e test for map sideinput, Fixes #1192 (#1211)
 * [7b32af34](https://github.com/numaproj/numaflow/commit/7b32af34a7ab545bdc1b37b15f152538b83f5f1b) feat: get current status of ISB service (#1199)
 * [5c7fc90e](https://github.com/numaproj/numaflow/commit/5c7fc90e633a8114cb681884ceae1e66c1d688b7) Summary view fixes (#1253)
 * [53aed68d](https://github.com/numaproj/numaflow/commit/53aed68d49c0901230517504512e71745fb7fb2e) chore(deps): bump github.com/nats-io/nats-server/v2 from 2.9.19 to 2.9.23 (#1232)
 * [30382995](https://github.com/numaproj/numaflow/commit/303829955202784af8eee8bf37cc54d108f3b123) fix: updated div's with box and removed unwanted css (#1236)
 * [8b50f36c](https://github.com/numaproj/numaflow/commit/8b50f36ca12b5c5045d8a32c6d4b62ef4219d6f4) chore(deps): bump @babel/traverse from 7.23.0 to 7.23.2 in /ui (#1221)
 * [7a3ca76a](https://github.com/numaproj/numaflow/commit/7a3ca76a2ec7d3b0f06f7bc7e26e310b02685bb9) Update kafka.md (#1218)
 * [e49a1811](https://github.com/numaproj/numaflow/commit/e49a1811094824d3b004efb3905ce716956616ad) Update generator.md (#1217)
 * [9785eb07](https://github.com/numaproj/numaflow/commit/9785eb0707cad8d1384c78b58e12feb00f2ec48e) Update map.md (#1219)
 * [a874478a](https://github.com/numaproj/numaflow/commit/a874478a2920b5a68b6d246c56a3d468b5dedc5e) feat: UI 1.0 CRUD (#1181)
 * [d34fcc47](https://github.com/numaproj/numaflow/commit/d34fcc47fcfa94fc5ad2d16ca51a27d9c39748da) fix: get isbsvc kind apiversion (#1220)
 * [74f4d980](https://github.com/numaproj/numaflow/commit/74f4d9809fde734cfca87db3675ec3d9d5e03ac7) Update kafka.md (#1215)
 * [7d5fe51f](https://github.com/numaproj/numaflow/commit/7d5fe51ff7b1dfc6d95bb3688e03fa207d412ba2) Update generator.md (#1214)
 * [b6adac15](https://github.com/numaproj/numaflow/commit/b6adac15cf62d82402f4bf6dff8fa6ee06f8b5a1) Update overview.md (#1213)
 * [0748449d](https://github.com/numaproj/numaflow/commit/0748449d9d1ce094a23a557c8951f5e0e34d8f56) feat: add udsource python e2e (#1204)
 * [fea29657](https://github.com/numaproj/numaflow/commit/fea29657f22ff89bccbf9f389ab983b1d901ddd9) doc: roadmap (#1208)
 * [d7630553](https://github.com/numaproj/numaflow/commit/d76305532bf5b920a13c508794f4ed61e30f6fb4) Namespace card status bar changes 0.11 (#1206)
 * [19a523aa](https://github.com/numaproj/numaflow/commit/19a523aa89df2e44a43e77e258ebf24b75de56ea) feat: added unit tests for PipelineCard component (#1205)
 * [6a50f130](https://github.com/numaproj/numaflow/commit/6a50f1301e4a76236aee58b73dd6b0cf7cc3b761) pods component error fix (#1203)
 * [a07e78f7](https://github.com/numaproj/numaflow/commit/a07e78f73032618296823a5bbea2dcf222122909) fix: updated image styles to a class (#1202)
 * [71c048eb](https://github.com/numaproj/numaflow/commit/71c048ebe39d08389e88087673ce514f2f6c6bc6) feat: Changed the status bar component to an icon based component (#1198)
 * [13264e3f](https://github.com/numaproj/numaflow/commit/13264e3fedd2bcb0c50b290db383943db50b64f3) fix(SERVER): restructure pod details component (#1189)
 * [1bbdcad8](https://github.com/numaproj/numaflow/commit/1bbdcad88ca9bc225ca8e38898c190e7cc5724d9) feat: updated the legend to a collapsible one on the top left (#1196)
 * [910243bd](https://github.com/numaproj/numaflow/commit/910243bd7998cc088c5cad296ec4427f6c585f50) chore(deps): bump golang.org/x/net from 0.12.0 to 0.17.0 (#1190)
 * [57af20e5](https://github.com/numaproj/numaflow/commit/57af20e5ce7291757c171629ccdee82b8532d384) feat: API Delete ISBSVC validation (#1182)
 * [2e1fd701](https://github.com/numaproj/numaflow/commit/2e1fd70177daf8f2c6fa47b2bf5f00b7fca50897) added BCubed to the user list (#1184)
 * [b962f8e3](https://github.com/numaproj/numaflow/commit/b962f8e3164d2fd9bb8b3806f511decbe11a4244) feat: add timeout for pausing pipeline. Fixes #992 (#1138)
 * [ef62c5ca](https://github.com/numaproj/numaflow/commit/ef62c5ca6a1127ee8d58b61737770185891f49cc) feat: Jetstream support for replica of 1 Fixes #944 (#1177)
 * [80294989](https://github.com/numaproj/numaflow/commit/80294989be83478520f96d6f4ad911839cbb9f43) doc: minor clean up of JOIN doc (#1175)
 * [d4b5f1b2](https://github.com/numaproj/numaflow/commit/d4b5f1b2600c52264a3fb90504af8dc53740f99b) fix: incorrect side inputs watch logic (#1164)
 * [00e6b6ae](https://github.com/numaproj/numaflow/commit/00e6b6ae691fda77f71c8932997dea994693576d) fix: not considered as back pressured when onFull is discardLatest (#1153)
 * [8b0d8cef](https://github.com/numaproj/numaflow/commit/8b0d8cef902442cc7302f5dbe9577b00e085abaf) fix(SERVER): handle states when status is unknown (#1154)
 * [31b1aacd](https://github.com/numaproj/numaflow/commit/31b1aacdb1c335cd7c69dccdb83e7c1fdc7c58cc) fix(SERVER): fix key warning (#1152)

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

 * [fbf51b2d](https://github.com/numaproj/numaflow/commit/fbf51b2db59f259d925471d40eede930441d9e71) Update manifests to v0.11.0
 * [f33d614f](https://github.com/numaproj/numaflow/commit/f33d614fecc3affd1310415f01e36e2ff152b5ff) Namespace card status bar changes 0.11 (#1206)
 * [391b75dc](https://github.com/numaproj/numaflow/commit/391b75dc7cca37ba9000f2e253f0256d2abeccb7) feat: added unit tests for PipelineCard component (#1205)
 * [7bcc1f27](https://github.com/numaproj/numaflow/commit/7bcc1f278de7f1815bb103543e06418d321f2c02) pods component error fix (#1203)
 * [df28f937](https://github.com/numaproj/numaflow/commit/df28f937991a38ffb3daca5650ba166160b3dbc9) fix: updated image styles to a class (#1202)
 * [bbb7db77](https://github.com/numaproj/numaflow/commit/bbb7db77b8ed79c3fec70f6ccd2f126b79d7d19c) feat: Changed the status bar component to an icon based component (#1198)
 * [f6f20d6c](https://github.com/numaproj/numaflow/commit/f6f20d6c89c024e616b0d21406f08a5e5002c302) fix(SERVER): restructure pod details component (#1189)
 * [96a6002e](https://github.com/numaproj/numaflow/commit/96a6002eedb6d7570815f0c02b1666a912291f47) feat: updated the legend to a collapsible one on the top left (#1196)
 * [575605b4](https://github.com/numaproj/numaflow/commit/575605b42adc7e34227894aabe64635e4030b0e7) chore(deps): bump golang.org/x/net from 0.12.0 to 0.17.0 (#1190)
 * [bfec4fc5](https://github.com/numaproj/numaflow/commit/bfec4fc56d79948af025d28ce8657ab11a0c1ed6) added BCubed to the user list (#1184)
 * [1e0b25fd](https://github.com/numaproj/numaflow/commit/1e0b25fd0b7556af3333002c149d53c14602b50a) feat: add timeout for pausing pipeline. Fixes #992 (#1138)
 * [ae232764](https://github.com/numaproj/numaflow/commit/ae23276463f55553ef2604d7eba55d796d994671) feat: Jetstream support for replica of 1 Fixes #944 (#1177)
 * [1e405b37](https://github.com/numaproj/numaflow/commit/1e405b37e92e2e898cc757f5d4ab9b67c5b7fef0) doc: minor clean up of JOIN doc (#1175)
 * [77c01811](https://github.com/numaproj/numaflow/commit/77c01811c5ab9df188a0559f51151e66141d9088) fix: incorrect side inputs watch logic (#1164)

### Contributors

 * Darshan Simha U
 * Derek Wang
 * Dillen Padhiar
 * Joel Millage
 * Vigith Maurice
 * dependabot[bot]
 * mshakira

## v0.11.0-rc2 (2023-10-03)

 * [8ae28af3](https://github.com/numaproj/numaflow/commit/8ae28af3fed4ed201f1eb40d3556377a04c9681f) Update manifests to v0.11.0-rc2
 * [f74bde39](https://github.com/numaproj/numaflow/commit/f74bde398d2e35e84c7752952597898956b9dc69) fix: not considered as back pressured when onFull is discardLatest (#1153)
 * [689aa295](https://github.com/numaproj/numaflow/commit/689aa2954ad279e7c91184dbf2f3690cde806fb2) fix(SERVER): handle states when status is unknown (#1154)
 * [f83ea00a](https://github.com/numaproj/numaflow/commit/f83ea00a8e93c51c2f914ca383a91b5b2f05984e) fix(SERVER): fix key warning (#1152)

### Contributors

 * Derek Wang
 * mshakira

## v0.11.0-rc1 (2023-10-03)

 * [1d89a12b](https://github.com/numaproj/numaflow/commit/1d89a12bc331e7daafaa30db540c5ac57ca50257) Update manifests to v0.11.0-rc1
 * [882bcef3](https://github.com/numaproj/numaflow/commit/882bcef3b3c2fbd37558f58bf5f424ead4be1aaa) feat: Numaflow UI 1.0 (#1077)
 * [a7adee1e](https://github.com/numaproj/numaflow/commit/a7adee1eabc66c530d295ded25b27005055d1537) fix: treat ALL user-defined source vertices as scalable (#1132)
 * [408ff389](https://github.com/numaproj/numaflow/commit/408ff389c56358a4ef6f98bf0f89cf1fe9bd415c) feat: add doc link checker (#1130)
 * [dae11a7f](https://github.com/numaproj/numaflow/commit/dae11a7fb8ad0b1650877bc864dc4c7c9506c996) extracting redis streams source (#1113)
 * [9d65e229](https://github.com/numaproj/numaflow/commit/9d65e229e0f8b92154dbbd2d50e707bf19f205aa) refactor: shared kubeconfig util (#1095)
 * [bb6f29b2](https://github.com/numaproj/numaflow/commit/bb6f29b23c8eea8b9db8b3293d04df5baaac9fff) fix: wrong api group in webhook rbac settings (#1086)
 * [4150eb28](https://github.com/numaproj/numaflow/commit/4150eb289355bf22a994a02a272b30fe4e93f50d) feat: set correct number of replices for some types of vertices after resuming pipeline (#1085)
 * [2182b83b](https://github.com/numaproj/numaflow/commit/2182b83b7f089821b3e780a61aef851bab34faa2) chore(deps): bump graphql from 16.6.0 to 16.8.1 in /ui (#1078)
 * [b2a377ca](https://github.com/numaproj/numaflow/commit/b2a377caf34e906f338cc3bff7d804d58195ba0b) feat: add forest validation for pipelines. Fixes #1002 (#1063)
 * [9035ba8b](https://github.com/numaproj/numaflow/commit/9035ba8b44677aa8805d17926520e3ce5a0d0ffe) feat: implement pipeline validation for unsupported states (#1043)
 * [4369d65f](https://github.com/numaproj/numaflow/commit/4369d65f5071591b033720ef941467650dac6afc) fix: message count read in forwarder (#1030)
 * [8d52d78b](https://github.com/numaproj/numaflow/commit/8d52d78bda240e96dabbbb9575f4c127d001d34d) fix(ci): Wrong test pod image tag used for running CI on release branch (#1041)
 * [6ed2c381](https://github.com/numaproj/numaflow/commit/6ed2c381e4b1d089e8442fee86eac331e30d3e83) fix: allow udsource pending api to return negative count to indicate PendingNotAvailable (#1040)
 * [c12e9094](https://github.com/numaproj/numaflow/commit/c12e9094131c033e3ab98d79b3d289577080ad88) refactor: re-arrange some of the rater implementations (#1036)
 * [abe332e6](https://github.com/numaproj/numaflow/commit/abe332e6d4cc09f3540440de17d9ec6225cf3c21) fix(docs): file names and links for side inputs (#1037)
 * [20fb7cc2](https://github.com/numaproj/numaflow/commit/20fb7cc29c1eae2fb148fd70b173c3c61beb7674) doc: Add side input docs (#1029)
 * [b373406c](https://github.com/numaproj/numaflow/commit/b373406ccdb4436f0bd6f568ebc3eb2f4f341fb9) doc: kill-switch when buffer is full (#1034)
 * [9f5127f0](https://github.com/numaproj/numaflow/commit/9f5127f04ceb95fe130387da192c834b5b1c6ffd) fix: Idle handler refactor (#1021)
 * [b1f8b026](https://github.com/numaproj/numaflow/commit/b1f8b0269669b58d90cc4982dd8bf28ee30c8fb0) fix: calculate processing rate for sink vertices (#1025)
 * [c01a1406](https://github.com/numaproj/numaflow/commit/c01a1406825131d5a6adfe9214d55fbab232a24b) feat: colored logs for UI with toggle for logs order (#1022)

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

 * [e5e2b619](https://github.com/numaproj/numaflow/commit/e5e2b6191a386a2e6fdb546e2d57b5ee69fb18ef) Update manifests to v0.10.1
 * [4702849c](https://github.com/numaproj/numaflow/commit/4702849c298b17f908b7de47d96ce94699661cc7) feat: implement pipeline validation for unsupported states (#1043)
 * [e36b3c6c](https://github.com/numaproj/numaflow/commit/e36b3c6ceb08e1c9a48cccce484643dadea630ff) fix: message count read in forwarder (#1030)
 * [01d9abee](https://github.com/numaproj/numaflow/commit/01d9abee0ba1faf2636f7dff4b5601be19c35bb6) fix: allow udsource pending api to return negative count to indicate PendingNotAvailable (#1040)
 * [8cbb4d67](https://github.com/numaproj/numaflow/commit/8cbb4d67331bc0ef8db5d52157ec7e2270b02711) refactor: re-arrange some of the rater implementations (#1036)
 * [b511effc](https://github.com/numaproj/numaflow/commit/b511effc2a85608ee3b372b0cb36ee77385afb7e) fix(docs): file names and links for side inputs (#1037)
 * [5b083c2d](https://github.com/numaproj/numaflow/commit/5b083c2d29fbb4740d3aadfc8622cadadfbe1d00) doc: Add side input docs (#1029)
 * [8d5c56f8](https://github.com/numaproj/numaflow/commit/8d5c56f8968eadc04c87cc0db308bc05ac418d87) doc: kill-switch when buffer is full (#1034)
 * [73db23a3](https://github.com/numaproj/numaflow/commit/73db23a3f834de99e9c9456d978692a3a8f55c71) fix: Idle handler refactor (#1021)
 * [7119ed96](https://github.com/numaproj/numaflow/commit/7119ed965723e3b55d1ac8484dcb7bc2e091c6bf) fix: calculate processing rate for sink vertices (#1025)
 * [6408137d](https://github.com/numaproj/numaflow/commit/6408137dad05d24690394a1143548da2bc618ccb) feat: colored logs for UI with toggle for logs order (#1022)
 * [2311260c](https://github.com/numaproj/numaflow/commit/2311260cce0aebf32552a1c55c890f7e5e606c6d) fix(ci): Wrong test pod image tag used for running CI on release branch (#1041)

### Contributors

 * Derek Wang
 * Dillen Padhiar
 * Juanlu Yu
 * Keran Yang
 * Sidhant Kohli
 * Vedant Gupta
 * Vigith Maurice

## v0.10.0 (2023-09-05)

 * [10d1cfde](https://github.com/numaproj/numaflow/commit/10d1cfde26f168e19dabc797485fc766d117f086) Update manifests to v0.10.0
 * [6f7c1f4a](https://github.com/numaproj/numaflow/commit/6f7c1f4a950a808a8e3f575a3223b1a0d5d44aaa) fix: seg fault inside controller (#1016)
 * [c2fdef16](https://github.com/numaproj/numaflow/commit/c2fdef16338e99b6cc26778705a7789937d7b49b) fix: reconcile headless services before pods (#1014)
 * [7d8b9087](https://github.com/numaproj/numaflow/commit/7d8b90874b37fcf91d4bf04432ece2816a04d513) fix: print version info when starting (#1013)
 * [247b89ed](https://github.com/numaproj/numaflow/commit/247b89ed9aa5e17142b982f955c01bfe2a6650ed) feat: join vertex UI support (#1010)
 * [aabb8af0](https://github.com/numaproj/numaflow/commit/aabb8af0a9dee4338c2ece6c35eaee4f4a169a20) feat: scaleUpCooldownSeconds and scaleDownCooldownSeconds to replace cooldownSeconds (#1008)
 * [ad647ab7](https://github.com/numaproj/numaflow/commit/ad647ab7549efac87160ca0a0f69a34153aaa887) chore(deps): bump @adobe/css-tools from 4.2.0 to 4.3.1 in /ui (#1005)
 * [92fbf7f1](https://github.com/numaproj/numaflow/commit/92fbf7f15dfd296dcbc20214753fb43550a9d1fd) fix: avoid unwanted watcher creation and reduce being stuck with udf is restarted (#999)
 * [bac06df0](https://github.com/numaproj/numaflow/commit/bac06df00748f2f1b84be210e2f79c6f280ebb9b) fix: missing edges on UI (#998)
 * [f90d4fe7](https://github.com/numaproj/numaflow/commit/f90d4fe7bff838cfed3001920965f33c57105f3d) feat: Add side input sdkclient and grpc  (#953)
 * [d99480a8](https://github.com/numaproj/numaflow/commit/d99480a890d00be99c4d2fc33f2856eae38db4d0) feat: implement user-defined source (#980)
 * [70685902](https://github.com/numaproj/numaflow/commit/706859025e2baaa1c77b77d9b83ec6bcf32129d0) fix: send keys for udsink (#979)
 * [8f32b9a3](https://github.com/numaproj/numaflow/commit/8f32b9a3e4ac4a55fb275f0407dc67f1f3523b4a) fix bulleted list (#977)
 * [1f33bf8b](https://github.com/numaproj/numaflow/commit/1f33bf8b45039ce235b930047ab3b77e0f1d8635) refactor: build wmstore and wmstorewatcher directly, and remove some unnecessary fields  (#970)
 * [4cea3444](https://github.com/numaproj/numaflow/commit/4cea3444ae1a375d2550ccd7b66e0541fece169c) feat: add vertex template to pipeline spec (#947)
 * [4a4ed927](https://github.com/numaproj/numaflow/commit/4a4ed9275c6da00588df72434f6e082e0bb0dd99) feat: Add side-input initializer and synchronizer (#912)
 * [d10f36e6](https://github.com/numaproj/numaflow/commit/d10f36e67581c76516554fd60acd400de45c2607) fix: npe when the ctx is canceled inside kv watcher (#942)
 * [6b1b3337](https://github.com/numaproj/numaflow/commit/6b1b3337c76bfdbe2a53173f97ce43ee993577ad) fix: retry logic for fetching last updated kv time (#939)
 * [e3da4a3e](https://github.com/numaproj/numaflow/commit/e3da4a3ef31191f61d75ebeab8ba5f76cfba0e17) fix: close the watermark fetcher and publishers after all the forwarders exit (#921)
 * [2d6112bf](https://github.com/numaproj/numaflow/commit/2d6112bf0c20baace28da76e6fb0ace3c3be01b5) Pipelines with Cycles: e2e testing, and pipeline validation (#920)
 * [5e0bf77e](https://github.com/numaproj/numaflow/commit/5e0bf77e6fbbb73ff267271064d7443dcdffac86) docs quick fixes (#919)
 * [0f8f7a17](https://github.com/numaproj/numaflow/commit/0f8f7a17b0a1e1259e771dafed38c71db3443543) docs updates (#917)
 * [b55566b8](https://github.com/numaproj/numaflow/commit/b55566b89b568b4211a467e98cb48a0c4b7ea884) feat: watermark delay in tooltip (#910)
 * [667ada75](https://github.com/numaproj/numaflow/commit/667ada75146ee4594ef6603fa06fb1c93e141a89) fix: removing WIP tag (#914)
 * [872aa864](https://github.com/numaproj/numaflow/commit/872aa8640c08c776b7cea9da4afa09a1a9098cc3) feat: emit k8s events for controller messages. Fixes #856 (#901)
 * [0fbdb7ab](https://github.com/numaproj/numaflow/commit/0fbdb7aba3fc6e15f6b81146fdf7a6acdae08868) fix: avoid potential deadlocks when operating UniqueStringList (#905)
 * [2c85ec43](https://github.com/numaproj/numaflow/commit/2c85ec439098a313f4c33829a0ef8d9db30a0ea0) refactor: avoid exposing internal data structures of pod tracker to the rater (#902)
 * [7e86306b](https://github.com/numaproj/numaflow/commit/7e86306bcb1c0cc66f08172d668ad5f14c7ca503) feat: Join Vertex (#875)
 * [85360f65](https://github.com/numaproj/numaflow/commit/85360f6528139721fff37048eccd0e605fc53418) fix: stabilize nats connection (#889)
 * [d4f8f594](https://github.com/numaproj/numaflow/commit/d4f8f59431e1322ce6a555018efd821801e69a12) doc: Update multi partition doc (#898)
 * [404672d6](https://github.com/numaproj/numaflow/commit/404672d68ed0777e94ee98ef008461bc5687d101) fix: Reduce idle WM unit test fix (#897)
 * [a1bbdedf](https://github.com/numaproj/numaflow/commit/a1bbdedf44289efc46773dd1d004b586d8663037) updated default version of Redis used for e2e (#891)
 * [85ee4b0d](https://github.com/numaproj/numaflow/commit/85ee4b0d8669f4b24fe202e6b9e7389e85826912) fix TestBuiltinEventTimeExtractor (#885)
 * [f3e1044e](https://github.com/numaproj/numaflow/commit/f3e1044eab7e2372dcdf2779d74e4ce8cb5f7cfb) chore(deps): bump word-wrap from 1.2.3 to 1.2.4 in /ui (#881)
 * [a02f29a7](https://github.com/numaproj/numaflow/commit/a02f29a710fd482b093193df11387e287d2c7c2e) fix: remove retry when the processor is not found. (#868)
 * [cfdeaa8a](https://github.com/numaproj/numaflow/commit/cfdeaa8a4b50ffae8edcfd3a940ba194e354f0b6) refactor: create a new data forwarder dedicated for source (#874)
 * [6d14998a](https://github.com/numaproj/numaflow/commit/6d14998a998e392590a8871da87514f5bffa6a46) feat: controller changes for Side Inputs support (#866)
 * [92db62a9](https://github.com/numaproj/numaflow/commit/92db62a907cb410a74ef20e066e5ed8aea10bf78) fix: highlight edge when buffer is full (#869)
 * [9c4e83c0](https://github.com/numaproj/numaflow/commit/9c4e83c0658dd319cf43eced249af965a9e8af18) fix: minor ui bugs (#861)
 * [b970b4cc](https://github.com/numaproj/numaflow/commit/b970b4cc7dfe90dd01a086a333e654275d5aeb7f) fix: release script for validating webhook (#860)
 * [7684aada](https://github.com/numaproj/numaflow/commit/7684aada2fb57458c572a088fbfc5c9ffc1a07e5) fix: use windower to fetch next window yet to be closed (#850)
 * [609d8b3c](https://github.com/numaproj/numaflow/commit/609d8b3ce2233b18f9c6debfc4c8ec65ee067dfa) feat: implement optional validation webhook. Fixes #817. (#832)
 * [3ae1cedb](https://github.com/numaproj/numaflow/commit/3ae1cedb3918d9936b0f2098b853f1d96d5e6e60) chore(deps): bump semver from 6.3.0 to 6.3.1 in /ui (#845)

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

 * [6141719f](https://github.com/numaproj/numaflow/commit/6141719f327e8f8d5b5176c75cd41b179622de96) Update manifests to v0.9.3
 * [022f8bfa](https://github.com/numaproj/numaflow/commit/022f8bfae8aa7e59abf0c795410f17312586e502) fix: seg fault inside controller (#1016)

### Contributors

 * Derek Wang
 * Yashash H L

## v0.9.2 (2023-08-23)

 * [6f81361f](https://github.com/numaproj/numaflow/commit/6f81361f8f36fad74208895aab599e63f9436b79) Update manifests to v0.9.2
 * [66c32197](https://github.com/numaproj/numaflow/commit/66c32197db97ed4335aacb0267558bc83026e788) fix: error when kv_watch with no keys (#981)

### Contributors

 * Derek Wang

## v0.9.1 (2023-08-11)

 * [4cbd729c](https://github.com/numaproj/numaflow/commit/4cbd729c91e0387781d00d97bfd8e0b61d0fd8c7) Update manifests to v0.9.1
 * [aa5e8ae3](https://github.com/numaproj/numaflow/commit/aa5e8ae3ef064fa7140480347c40c74c73b01f25) fix: npe when the ctx is canceled inside kv watcher (#942)
 * [e5a5cd6c](https://github.com/numaproj/numaflow/commit/e5a5cd6c0859c25cdf1110d4804ec4f88b0b068c) feat: watermark delay in tooltip (#910)

### Contributors

 * Derek Wang
 * Vedant Gupta
 * Yashash H L

## v0.9.0 (2023-08-02)

 * [8e4b6ca1](https://github.com/numaproj/numaflow/commit/8e4b6ca1337ff444b348be597e036728fca9d757) Update manifests to v0.9.0
 * [7424ae50](https://github.com/numaproj/numaflow/commit/7424ae50910c399227dfc4b1eae2d31c295513cc) feat: emit k8s events for controller messages. Fixes #856 (#901)
 * [d0bfac6d](https://github.com/numaproj/numaflow/commit/d0bfac6d5e130e6f9e1f97c873ad3ca404c3c2fd) fix: avoid potential deadlocks when operating UniqueStringList (#905)
 * [75c7f975](https://github.com/numaproj/numaflow/commit/75c7f975b13505ad1b9abe674a736db23d2021d5) fix: stabilize nats connection (#889)
 * [0db1238d](https://github.com/numaproj/numaflow/commit/0db1238db3f33684604fea3fa5b367e5d4f3a3c3) fix: Reduce idle WM unit test fix (#897)
 * [5073f1c8](https://github.com/numaproj/numaflow/commit/5073f1c81946d58a3757e4c658299ccdc9534d77) fix TestBuiltinEventTimeExtractor (#885)
 * [33b7d1d0](https://github.com/numaproj/numaflow/commit/33b7d1d08387903f59bcce2cf182879a4de54c79) fix: remove retry when the processor is not found. (#868)
 * [89b2d1c4](https://github.com/numaproj/numaflow/commit/89b2d1c4d0cd0e996469b5e8acb389845b626f53) fix: highlight edge when buffer is full (#869)
 * [8d49c0f6](https://github.com/numaproj/numaflow/commit/8d49c0f67a2cf905a48748525bb02dd50ab1bedc) fix: minor ui bugs (#861)
 * [9478e302](https://github.com/numaproj/numaflow/commit/9478e3026c71c2108166b2f94070819e7e232bba) fix: release script for validating webhook (#860)

### Contributors

 * Derek Wang
 * Dillen Padhiar
 * Juanlu Yu
 * Keran Yang
 * Vedant Gupta
 * Yashash H L

## v0.9.0-rc2 (2023-07-13)

 * [d0df669a](https://github.com/numaproj/numaflow/commit/d0df669a8bb9f07fffe1d5add792444ebfb33835) Update manifests to v0.9.0-rc2
 * [c8aaeff8](https://github.com/numaproj/numaflow/commit/c8aaeff8ca796b43279d9b784883d632bf4b8d32) fix: use windower to fetch next window yet to be closed (#850)
 * [bcda8dcf](https://github.com/numaproj/numaflow/commit/bcda8dcfcdbde7821c201b37f5d5ce99148a341c) feat: implement optional validation webhook. Fixes #817. (#832)
 * [e605504d](https://github.com/numaproj/numaflow/commit/e605504d40fce533c64569d0d30a8533df62299d) chore(deps): bump semver from 6.3.0 to 6.3.1 in /ui (#845)

### Contributors

 * Derek Wang
 * Dillen Padhiar
 * Yashash H L
 * dependabot[bot]

## v0.9.0-rc1 (2023-07-11)

 * [40f45410](https://github.com/numaproj/numaflow/commit/40f45410780c51c8109650e2835a105543d3f77c) Update manifests to v0.9.0-rc1
 * [f5276dbb](https://github.com/numaproj/numaflow/commit/f5276dbb227a8ff3b0a21b257790a7eb2f282911) fix: pod tracker logic for calculating processing rate (#838)
 * [db06e7e4](https://github.com/numaproj/numaflow/commit/db06e7e4b36e608fc19e578e71adc7d96c2d8197) chore(deps): bump tough-cookie from 4.1.2 to 4.1.3 in /ui (#839)
 * [b660b6d9](https://github.com/numaproj/numaflow/commit/b660b6d99118c945a6a466f3387fbade4a1984e0) fix: resource leak inside daemon server (#837)
 * [1f19a742](https://github.com/numaproj/numaflow/commit/1f19a7421d6194aa25ed9fa2d3a2327ba6bbf449) feat: capability to increase max message size (#835)
 * [c61ce319](https://github.com/numaproj/numaflow/commit/c61ce319f133aad18e6cf3dd597fde11b991b22f) doc: update roadmap (#830)
 * [aca1c9bf](https://github.com/numaproj/numaflow/commit/aca1c9bf4495ad986c46bfd0cd58f2556cea9599) feat: add stragglers (late data) into the window is window is open (#824)
 * [0155b4a5](https://github.com/numaproj/numaflow/commit/0155b4a539e95d9254d9bc6bd57a29436f01ea12)  fix(docs): fixed some incorrect docs and renamed a timeExtractionFilter arg (#814)
 * [dd060cb8](https://github.com/numaproj/numaflow/commit/dd060cb80637ad5904d3000785c0214a80894e7e) feat: rater changes to track processing rate per partition (#805)
 * [541ceb20](https://github.com/numaproj/numaflow/commit/541ceb20f0105567f220b101efe775ee3a027a08) fix: metric to track watermark bug was wrongly tagged (#809)
 * [cf473151](https://github.com/numaproj/numaflow/commit/cf473151974b1d907be88addb40d9bb549a0bbd0) feat: autoscaling changes to support multi partition (#806)
 * [6a5ee1a5](https://github.com/numaproj/numaflow/commit/6a5ee1a5b8a3b237a3299a094f03c4ae79fa77de) fix: segmentation fault in daemon server (#804)
 * [2ce0ac90](https://github.com/numaproj/numaflow/commit/2ce0ac90ce8f274b78b6d78a8b9d9138d6e0777e) fix: Intermittent failure from Kafka to get consumer offsets (#803)
 * [32be7fc5](https://github.com/numaproj/numaflow/commit/32be7fc54de762c30fea5343f1f0a982c7b5f626) feat: support UI for multipartition edges (#789)
 * [97db1984](https://github.com/numaproj/numaflow/commit/97db1984ac5f14cd53e4d09b4ceb827f45b23f2e) refactor: remove redundant delta calculations for rater (#795)
 * [3406a130](https://github.com/numaproj/numaflow/commit/3406a13044308b81a7e9f1d462d05f99e059bbb3) fix: select pods not in evicted status (#786)
 * [a9204fbc](https://github.com/numaproj/numaflow/commit/a9204fbc8b069357ded8f7a2ea9f8d7f7b767854) feat: combine built-in UDTransformers for filter and eventTime assignment (#783)
 * [85955e30](https://github.com/numaproj/numaflow/commit/85955e306d899992ce6118069f804f02f2f28bef) feat: support multi-partitioned edges (#751)
 * [5ce6936d](https://github.com/numaproj/numaflow/commit/5ce6936d161d45e5050a038e1eaaf54b229a4eb0) fix: duplicate ui served from gin Router (#781)
 * [e9ea7d85](https://github.com/numaproj/numaflow/commit/e9ea7d855682d91e1a0d30d4c8694bf875018d54) fix: unexpected high processing rates (#780)
 * [f60b8ab9](https://github.com/numaproj/numaflow/commit/f60b8ab936b76020523b22a28dc54450f76d75d1) chore(deps): bump github.com/gin-gonic/gin from 1.9.0 to 1.9.1 (#772)
 * [466e3804](https://github.com/numaproj/numaflow/commit/466e380484f08b6f74f8dfd4f8c47a9ab4196599) feat: gRPC error handling (#744)
 * [9aff2bd8](https://github.com/numaproj/numaflow/commit/9aff2bd88bc2eaebf61d6898571f2ca601c3afe7) feat: forwardAChunk to support multi partitioned edges (#757)
 * [f3273170](https://github.com/numaproj/numaflow/commit/f32731709c24cf26df9ce523f9e026cd0fbac51a) fix: pipeline view fix (#755)
 * [037c9a61](https://github.com/numaproj/numaflow/commit/037c9a612b6d138014f6d83ab56fdb92c89227de) fix: toVertexPartitions for reduce was incorrectly populated to 1 (#756)
 * [099b914a](https://github.com/numaproj/numaflow/commit/099b914ac776625d3e64d1db3e05be1543376587) feat: use metrics to calculate vertex processing rate (#743)
 * [59880e97](https://github.com/numaproj/numaflow/commit/59880e97c2ae850e00f280ddb7551d58197e48e4) feat: enable streaming message to next vertex when batch size is 1 (#709)
 * [160b9414](https://github.com/numaproj/numaflow/commit/160b9414085138c67b0c421523a29844a6559b63) fix: use int32 for message length (#750)
 * [e383ee2f](https://github.com/numaproj/numaflow/commit/e383ee2f6d09146c6ccbf5c76ea9627e63b779fa) feat: using one bucket for partitioned reduce watermark propagation (#742)
 * [ba1f493d](https://github.com/numaproj/numaflow/commit/ba1f493dc57acd2ba080a8107adbaa6b7c27fa7a) fix(test): flakey test (#738)
 * [f0c83291](https://github.com/numaproj/numaflow/commit/f0c8329101bddd6f4f7e06d95ce6c283c45311c6) refactor: buffer, edge, bucket (#733)
 * [eb9a7c4c](https://github.com/numaproj/numaflow/commit/eb9a7c4cdfb7b4dd2e5f30190587b5a184fe2c06) feat: change baseHref for Numaflow UI. Fixes #375. (#698)
 * [b1f639e7](https://github.com/numaproj/numaflow/commit/b1f639e7e48da7f6ebd98bf164a12ed8335f2be6) fix: let kafka source crash and restart when there is any server side error (#735)
 * [c8aafc7f](https://github.com/numaproj/numaflow/commit/c8aafc7f7da7008c6c83fae8d70e49bc045a2f0e) feat: Autoscale for Redis Streams Source (#726)
 * [d8074ec1](https://github.com/numaproj/numaflow/commit/d8074ec16ffeaccb6e60b5eab0ccb6898cf385f9) feat: Redis7 as an ISB svc (#717)
 * [3dd2a31d](https://github.com/numaproj/numaflow/commit/3dd2a31db9835d9052929f11ff42323cdbf865fc) chore(deps): bump github.com/gin-gonic/gin from 1.8.1 to 1.9.0 (#724)
 * [15a229b5](https://github.com/numaproj/numaflow/commit/15a229b5191820e96f6d0f251dfc33f704ea7cd7) docs(proposal): edges, buffers and buckets (#704)
 * [714c8036](https://github.com/numaproj/numaflow/commit/714c80360f9a77aa060c058b1361e37fd682b629) chore(doc): update README with demo (#718)
 * [431778de](https://github.com/numaproj/numaflow/commit/431778def1f15c2cd7b3b119f609662ef7464212) doc: add overview (#713)
 * [f518d99c](https://github.com/numaproj/numaflow/commit/f518d99c8bb8561594b3099f3da19f30661e50e2) feat: allowedLateness to support late data ingestion (#703)
 * [1efac426](https://github.com/numaproj/numaflow/commit/1efac4264c185ad2fa806f69f02a48cfe902d58a) fix: allow late message as long as window is not closed (#696)
 * [3da84fa5](https://github.com/numaproj/numaflow/commit/3da84fa519a10cee0dddc37a31bd8ed4e3219482) fix: add wal dir x permission (#689)
 * [3a91cc52](https://github.com/numaproj/numaflow/commit/3a91cc52f507d310ad8a926e13ad30e691e05df3) chore(doc): refactor doc struct (#685)

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

 * [4b119387](https://github.com/numaproj/numaflow/commit/4b11938700d8dadc8d3e4ba47a7a04e11659c3bd) Update manifests to v0.8.1
 * [67277b79](https://github.com/numaproj/numaflow/commit/67277b794766dadc1a77bc77baa70277fdacf07c) fix: pipeline view fix (#755)
 * [7cb399e9](https://github.com/numaproj/numaflow/commit/7cb399e96676e43af942baf9ef2c20165c16e41f) fix: toVertexPartitions for reduce was incorrectly populated to 1 (#756)
 * [16067af2](https://github.com/numaproj/numaflow/commit/16067af2b057df5f7a09f2de90f93cadc86a651e) feat: use metrics to calculate vertex processing rate (#743)
 * [11cd8e9f](https://github.com/numaproj/numaflow/commit/11cd8e9f2bf960cfe2516e900071e4e552c53ee2) feat: enable streaming message to next vertex when batch size is 1 (#709)
 * [a5058840](https://github.com/numaproj/numaflow/commit/a5058840735f9d78c5ca9ae408c66375290dbfa9) fix: use int32 for message length (#750)
 * [c602a520](https://github.com/numaproj/numaflow/commit/c602a5203d397585030362a3db9b9ee60dfac572) feat: using one bucket for partitioned reduce watermark propagation (#742)
 * [ccf79c6d](https://github.com/numaproj/numaflow/commit/ccf79c6d548aa5043bdc2a11de79cc861da250d6) fix(test): flakey test (#738)
 * [af8e3346](https://github.com/numaproj/numaflow/commit/af8e3346e08d3076c71f6bb649c7af8c2574943a) refactor: buffer, edge, bucket (#733)
 * [d57bfed4](https://github.com/numaproj/numaflow/commit/d57bfed42a457264087190d54662916ec9fc1589) feat: change baseHref for Numaflow UI. Fixes #375. (#698)
 * [37dfae58](https://github.com/numaproj/numaflow/commit/37dfae58a8719f5fe806e442c0701b8f2dc5e11c) fix: let kafka source crash and restart when there is any server side error (#735)
 * [b7a0dda5](https://github.com/numaproj/numaflow/commit/b7a0dda51b185b7d0e819bbb70a97511552b2c49) feat: Autoscale for Redis Streams Source (#726)
 * [5654e0af](https://github.com/numaproj/numaflow/commit/5654e0affe3910ee0fa7497f684fa3e1ab6babe2) feat: Redis7 as an ISB svc (#717)
 * [9e8d8cc4](https://github.com/numaproj/numaflow/commit/9e8d8cc42428cc5b8d84d9668a19a70b341a8914) chore(deps): bump github.com/gin-gonic/gin from 1.8.1 to 1.9.0 (#724)

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

 * [e57ca739](https://github.com/numaproj/numaflow/commit/e57ca739d3ca7539f2090c2509580174269b0e44) Update manifests to v0.8.0
 * [652be8d6](https://github.com/numaproj/numaflow/commit/652be8d60c13c6b43f9aabe0f317876dcbdcda23) feat: allowedLateness to support late data ingestion (#703)
 * [8e7e3b61](https://github.com/numaproj/numaflow/commit/8e7e3b61efaf2c1b695d4c6e2c14a4c631b8baee) fix: allow late message as long as window is not closed (#696)
 * [fcaed47d](https://github.com/numaproj/numaflow/commit/fcaed47d0a4654376bed2e500274dee370e7f848) fix: add wal dir x permission (#689)
 * [aae08fa1](https://github.com/numaproj/numaflow/commit/aae08fa1b93cb156af1b977608e7bf73dabcd42f) chore(doc): refactor doc struct (#685)

### Contributors

 * Derek Wang
 * Vigith Maurice

## v0.8.0-rc1 (2023-04-14)

 * [ca88313d](https://github.com/numaproj/numaflow/commit/ca88313d52c67700b7d7d74a6e326235783a06f6) Update manifests to v0.8.0-rc1
 * [b83525df](https://github.com/numaproj/numaflow/commit/b83525dfa60cabea55797ad6a0d27f0e43f26d88) feat: introducing tags for conditional forwarding (#668)
 * [a6e81746](https://github.com/numaproj/numaflow/commit/a6e81746b43787a42a4a0e78808ea99aef5bfc9a) feat: expose cpu/mem info to sidecar containers (#678)
 * [c7b853aa](https://github.com/numaproj/numaflow/commit/c7b853aad2cc4189e5666ee4e21b9c60913f3ad0) feat: Redis Streams source fixes (#669)
 * [2f73b5b8](https://github.com/numaproj/numaflow/commit/2f73b5b84b7ed771cbd18f76c9455aa86003b8d3) fix: skip empty Kafka partitions when calculating pending count (#666)
 * [eeb37d8b](https://github.com/numaproj/numaflow/commit/eeb37d8b6842e29e17491cde327cad503d06f143) feat: support for multi keys (#658)
 * [91b516b8](https://github.com/numaproj/numaflow/commit/91b516b8ad6ad3940f8721613a7d12c982ce6ed2) feat: Adds SASL (plain and gssapi) support for kafka sink (#656)
 * [196f887d](https://github.com/numaproj/numaflow/commit/196f887d31378a6ad6868bb1eb5e837512c31b04) fix: vertex overlapping watermark (#660)
 * [0db3248d](https://github.com/numaproj/numaflow/commit/0db3248d0e0618f2d4b6fac9266414681e2bedb0) feat: incremental search and namespace preview in search bar (#654)
 * [46bb8750](https://github.com/numaproj/numaflow/commit/46bb875053f82b0b8ee21cbcd91d55d72bef9d17) feat: integrate serde WAL (#650)
 * [b0560876](https://github.com/numaproj/numaflow/commit/b056087626034f6775f8da8def906a4e9d0131c5) fix: unit test (#653)
 * [1b0ea088](https://github.com/numaproj/numaflow/commit/1b0ea088134b464252f3c2296556bd97fa4acb9b) feat: handle idle watermark for reduce vertex (#627)
 * [33882628](https://github.com/numaproj/numaflow/commit/33882628adadb2f8f36c5519c41c50398ffd86eb) feat: Redis streams source (#628)
 * [d85bf93f](https://github.com/numaproj/numaflow/commit/d85bf93fd5f99ce37d4e0c36cc27ae417e938fec) feat: Adds SASL (plain and gssapi) support for kafka source (#643)
 * [60bb2bb9](https://github.com/numaproj/numaflow/commit/60bb2bb96a66b495a8243433f9e31c87ee303c9e) feat: namespace scope api and disable namespace search on UI (#638)
 * [38b5a9ec](https://github.com/numaproj/numaflow/commit/38b5a9ec8115f4bb77f34a793fcbaf3e701e0b19) fix: GetHeadWatermark Logic (#636)
 * [927b95cd](https://github.com/numaproj/numaflow/commit/927b95cd007b71a1494cf11cc9614048e5b45898) feat: enable edge-level kill switch to drop messages when buffer is full, for the non-reduce forwarder (#634)
 * [0c79113c](https://github.com/numaproj/numaflow/commit/0c79113c53619e30bc51b7687f9cdc313f752679) fix: IdleWatermark unit test (#640)
 * [924ad33e](https://github.com/numaproj/numaflow/commit/924ad33eb516eda4295d7af276bff130216e3e0a) fix: desired replicas should not be greater than pending (#639)
 * [150c5c23](https://github.com/numaproj/numaflow/commit/150c5c23e6898be41937d8f0331bef7742ce7baa) fix: add timeout to the test (#618)
 * [2f112fb2](https://github.com/numaproj/numaflow/commit/2f112fb29ad4723937f95d73a93b16526f6aec20) feat: kustomize integration (#637)
 * [5062aac6](https://github.com/numaproj/numaflow/commit/5062aac603421c4d95a94133852643104c9f3337) fix: exclude ack pending messages (#631)
 * [e533ba35](https://github.com/numaproj/numaflow/commit/e533ba3572dffaf58e807000b00fe84b48b4539e) feat: UI error component (#613)
 * [20aaca9d](https://github.com/numaproj/numaflow/commit/20aaca9de98db83a43ebb59773c425cd269759d1) fix: do not update status.replicas until pod operation succeeds (#620)
 * [cc62c81c](https://github.com/numaproj/numaflow/commit/cc62c81c72b4d5025af35a1467c59cb3c77804a4) feat: track and expose oldest work yet to be done to the reduce loop (#617)
 * [4bbe80bb](https://github.com/numaproj/numaflow/commit/4bbe80bb39deb4b723a4f6e239581a7791c5cb9f) feat: handle watermark barrier for map vertex (#607)
 * [f9f05442](https://github.com/numaproj/numaflow/commit/f9f05442fdf5f2c498066b45e2e10dd36193fb10) fix: corrected reduce vertex replica number. Fixes #593 (#616)
 * [c9815132](https://github.com/numaproj/numaflow/commit/c9815132b1f747be09af7c7e005f93c050ecdfca) feat: add API for pipeline status check. Fixes #407. (#599)
 * [5282766d](https://github.com/numaproj/numaflow/commit/5282766d074e57fd5581650f372858e5c8b7519c) chore(deps): bump webpack from 5.74.0 to 5.76.1 in /ui (#610)
 * [927bfc04](https://github.com/numaproj/numaflow/commit/927bfc047033098480f0f861fabcee3d60b60daa) feat: use randomized shuffle using vertex name as the seed (#601)
 * [d667d799](https://github.com/numaproj/numaflow/commit/d667d799fc1e56eb46d33fa4daf4dfb5cf5367a0) fix: ack the dropped messages as well (#603)
 * [64e17d88](https://github.com/numaproj/numaflow/commit/64e17d884a325f04171afe642c17aff3818c79ab) feat: enable controller HA (#602)
 * [20ba722f](https://github.com/numaproj/numaflow/commit/20ba722f6c478d5d07a84f45ff9869216e0a246b) feat: expose dnspolicy and dnsconfig to pod template (#598)
 * [abfdd78f](https://github.com/numaproj/numaflow/commit/abfdd78fe138721f6311c0260a348706f68469f2) Chore: tickgen changes to test reduce pipelines (#587)
 * [b2f8a12a](https://github.com/numaproj/numaflow/commit/b2f8a12a49405a8a29e530fc50f01b410e0ef3de) feat: bidirectional streaming (#553)
 * [148663e7](https://github.com/numaproj/numaflow/commit/148663e7583af02dcb665b922d687e215ab7a5df) feat: use customized binary serde for nats message payload (#585)
 * [8d339b68](https://github.com/numaproj/numaflow/commit/8d339b68421e8490425a2168b969aec01823949b) fix: Idle watermark fix for read batch size > 0 and partial idle outgoing edges (#575)
 * [87ab1e3d](https://github.com/numaproj/numaflow/commit/87ab1e3d34d4e550fe6973225036ce891066fc5e) feat: implement watermark propagation for source data transformer (#557)
 * [d561867f](https://github.com/numaproj/numaflow/commit/d561867f69d1bac96eb26624e830c7193424d091) feat: namespace search (#559)
 * [1b4800af](https://github.com/numaproj/numaflow/commit/1b4800afe96797cea557ab15ccd9ee0af03965d3) fix: refine log for buffer validation. Fixes #185 (#573)
 * [4eb27eff](https://github.com/numaproj/numaflow/commit/4eb27effe0fbcfc0f369b5dbeb185aab182fcc87) feat: add readiness and liveness check for daemon server. Fixes #543 (#571)
 * [8ae2116d](https://github.com/numaproj/numaflow/commit/8ae2116d280fb1faf30525d2eeb1f1c72fd84986) feat: marshal/unmarshal binary for read message (#565)
 * [a0505e67](https://github.com/numaproj/numaflow/commit/a0505e67f71db63cfc9bbbd4b83d27f773cdae54) chore(deps): bump golang.org/x/net from 0.0.0-20220722155237-a158d28d115b to 0.7.0 (#568)
 * [fbf36894](https://github.com/numaproj/numaflow/commit/fbf36894e26f0f832077dfc0baf35b88fbe579e5) chore(deps): bump golang.org/x/text from 0.3.7 to 0.3.8 (#567)
 * [92c8009d](https://github.com/numaproj/numaflow/commit/92c8009dedd760beb3a01331e3db05a726157718) feat: expose image pull policy to user defined containers (#563)
 * [88a41c2b](https://github.com/numaproj/numaflow/commit/88a41c2b0404975d875b12f87c4e856b7168defc) fix: typos in reduce examples (#556)
 * [97567f3c](https://github.com/numaproj/numaflow/commit/97567f3cc7901845ea59d4a76741e13df6989048) feat: edge-watermark (#537)
 * [1e06ba2f](https://github.com/numaproj/numaflow/commit/1e06ba2f708c6061f36f8e870fcde257a2014984) feat: enable envFrom for user defined containers (#554)
 * [0dc85f69](https://github.com/numaproj/numaflow/commit/0dc85f694e6f1048d4616705b3fa85fdb5bda9cd) feat: remove secret watch privilege dependency (#542)
 * [8b7e397e](https://github.com/numaproj/numaflow/commit/8b7e397e5ffbb28f70f01b20446c909ced7a4f25) fix: Use a copied object to update (#541)
 * [943e7bd8](https://github.com/numaproj/numaflow/commit/943e7bd83dbdee38af28c4ee1c445ba31564d53f) chore(deps): bump github.com/emicklei/go-restful from 2.9.5+incompatible to 2.16.0+incompatible (#539)
 * [93753c15](https://github.com/numaproj/numaflow/commit/93753c1527104f86748444a321663d52b15dea98) feat: improve reduce performance (#501)
 * [b502fa93](https://github.com/numaproj/numaflow/commit/b502fa93c5f2a59aa38c8ed7d3fa626aff39436c) feat: Offset time idle watermark put (#529)
 * [4551505c](https://github.com/numaproj/numaflow/commit/4551505c5ca59b56018827aa657995766cd24e94) fix: securityContext not applied to container templates (#528)
 * [2727e62a](https://github.com/numaproj/numaflow/commit/2727e62ac3271c7178bd6bc797c68979b1617538) feat: idle watermark v0 (#520)
 * [1e34e315](https://github.com/numaproj/numaflow/commit/1e34e31542816f99ba377e6d85121ac6a6df9b4a) feat: Reduce UI Support (#500)
 * [39ecae42](https://github.com/numaproj/numaflow/commit/39ecae427080800cb062bd65713b16b37ff596bf) feat: enable RuntimeClassName for vertex pod (#519)
 * [b965318d](https://github.com/numaproj/numaflow/commit/b965318d21e264682b868bff9268d7ecf54a8c92) feat: add builtin filter and event time extractor for source transformer (#517)
 * [077771ce](https://github.com/numaproj/numaflow/commit/077771cef79e798241f2cda5e01385834f451ee6) chore(deps): bump ua-parser-js from 0.7.32 to 0.7.33 in /ui (#507)
 * [58b12ec3](https://github.com/numaproj/numaflow/commit/58b12ec3eb996045428df215011da1d88a3aa8a5) Add an e2e test for source data transformer (#505)
 * [2af91933](https://github.com/numaproj/numaflow/commit/2af91933ee9b5dfc3feb1dfaebe81364c22a949d) feat: Implement source data transformer and apply to all existing sources (#487)
 * [a3024f4e](https://github.com/numaproj/numaflow/commit/a3024f4e26c5a3467c3b526f7083ab57948a8678) fix: -ve metrics and return early if isLate (#495)
 * [22986153](https://github.com/numaproj/numaflow/commit/2298615374ff699179384627cfe6e5b13da793af) fix: JetStream context KV store/watch fix (#460)
 * [2177d621](https://github.com/numaproj/numaflow/commit/2177d6213184d4a0eebc69981c1d363848d09b42) doc: reduce persistent store (#458)
 * [3c621207](https://github.com/numaproj/numaflow/commit/3c62120725aa940831b901881a2d17192bcb0ee8) doc: reduce documentation (#448)
 * [784fe15c](https://github.com/numaproj/numaflow/commit/784fe15cba31ccdfc986bf7bd8a526ef409805b0) chore(deps): bump json5 from 1.0.1 to 1.0.2 in /ui (#454)
 * [659a98b5](https://github.com/numaproj/numaflow/commit/659a98b5e8ffcbc3fc62775f8ce21df8f3ff2f6b) refactor: simplify http request construction in test cases (#444)
 * [cc9c194b](https://github.com/numaproj/numaflow/commit/cc9c194bf05671071f6416dea4d0b0d92f67617f) refactor: use exact matching instead of regex to perform e2e data validation. (#443)
 * [f7f712b3](https://github.com/numaproj/numaflow/commit/f7f712b34a640103cec327cf862ff2a11ef6a4c7) doc: windowing fixed and sliding (#439)
 * [9ad504b5](https://github.com/numaproj/numaflow/commit/9ad504b5791ddd22516aabe721fb94dd252dc289) refactor: move redis sink resources creation to E2ESuite (#437)
 * [0148258d](https://github.com/numaproj/numaflow/commit/0148258daa7b87fca65f04f70b8d769bb7796468) refactor: a prototype for enhancing E2E test framework (#424)
 * [8579dc67](https://github.com/numaproj/numaflow/commit/8579dc67516ec6c85f61c0b3e473d02c688296ff) feat: pipeline watermark (#416)

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

 * [68a14793](https://github.com/numaproj/numaflow/commit/68a14793ff698f68d45e404e0269de32fd3c8ed3) Update manifests to v0.7.3
 * [a17a41df](https://github.com/numaproj/numaflow/commit/a17a41df7324c7f72b4dcd09592593143c57fc76) feat: integrate serde WAL (#650)
 * [096c6acf](https://github.com/numaproj/numaflow/commit/096c6acf6b82cf540e387c731b96ddad8e9861a2) fix: unit test (#653)
 * [cce50ffd](https://github.com/numaproj/numaflow/commit/cce50ffd16a39931fe031603e53256ffe593ccae) feat: handle idle watermark for reduce vertex (#627)
 * [968cc5f5](https://github.com/numaproj/numaflow/commit/968cc5f50acec697a482c2bcdd93935c71d202c2) feat: Redis streams source (#628)
 * [38baae8a](https://github.com/numaproj/numaflow/commit/38baae8aa1b4a7152b2aa9d537b6934fc1cb2f11) feat: Adds SASL (plain and gssapi) support for kafka source (#643)
 * [563b85b1](https://github.com/numaproj/numaflow/commit/563b85b19a5ec9f3c58ad86d6cdee1bab4849d12) feat: namespace scope api and disable namespace search on UI (#638)
 * [c8194690](https://github.com/numaproj/numaflow/commit/c8194690f383624fd0e27c75b87069266fb6590d) fix: GetHeadWatermark Logic (#636)
 * [d37f4db5](https://github.com/numaproj/numaflow/commit/d37f4db5c29155bf084ca8b84a642020041c92af) feat: enable edge-level kill switch to drop messages when buffer is full, for the non-reduce forwarder (#634)
 * [a0dce69a](https://github.com/numaproj/numaflow/commit/a0dce69a35c11a54a1c399e77a91bacb1a646882) fix: IdleWatermark unit test (#640)
 * [f840e1c3](https://github.com/numaproj/numaflow/commit/f840e1c30a94d8d0b0644304b2bec71bbb565869) fix: desired replicas should not be greater than pending (#639)
 * [0ca5630e](https://github.com/numaproj/numaflow/commit/0ca5630e6695fe9d526b8a1a000e44211a7610a2) fix: add timeout to the test (#618)
 * [b049f0b4](https://github.com/numaproj/numaflow/commit/b049f0b4e37fd9ff37bfffb18ab39baf5acf0d09) feat: kustomize integration (#637)
 * [01305ea3](https://github.com/numaproj/numaflow/commit/01305ea3ec8e34fdd92a239bcf4ca2f63cc00032) fix: exclude ack pending messages (#631)
 * [4ec4b3d7](https://github.com/numaproj/numaflow/commit/4ec4b3d7f450ed8f7d5da5838223f560750dde03) feat: UI error component (#613)
 * [90ca505b](https://github.com/numaproj/numaflow/commit/90ca505bb027ec9d1ae79c728d6af2a86110edae) fix: do not update status.replicas until pod operation succeeds (#620)
 * [339db43b](https://github.com/numaproj/numaflow/commit/339db43b65136a2339d77b12edca166e78d47845) feat: track and expose oldest work yet to be done to the reduce loop (#617)
 * [1ce4c383](https://github.com/numaproj/numaflow/commit/1ce4c383caf29384e937f6686e52f6561f8d93d2) feat: handle watermark barrier for map vertex (#607)
 * [90dbe1fa](https://github.com/numaproj/numaflow/commit/90dbe1fab20ad1421706bba772ae9352d16e674b) fix: corrected reduce vertex replica number. Fixes #593 (#616)
 * [a155f2af](https://github.com/numaproj/numaflow/commit/a155f2af09522b8fb87a14af3559e1a2a74ddb57) feat: add API for pipeline status check. Fixes #407. (#599)
 * [d9e3a56f](https://github.com/numaproj/numaflow/commit/d9e3a56f6ed25f4baf05f22ab058ac393a3ab4a3) chore(deps): bump webpack from 5.74.0 to 5.76.1 in /ui (#610)

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

 * [1196a24b](https://github.com/numaproj/numaflow/commit/1196a24b7096273b60c072e803a336c79eef2c5b) Update manifests to v0.7.2
 * [a6f64f8d](https://github.com/numaproj/numaflow/commit/a6f64f8d93f20bfa91b592fd00333068633bc2c2) feat: use randomized shuffle using vertex name as the seed (#601)
 * [16b20a35](https://github.com/numaproj/numaflow/commit/16b20a357ade0c0bae9edb0461ee01340025eb7f) fix: ack the dropped messages as well (#603)
 * [4cdef174](https://github.com/numaproj/numaflow/commit/4cdef17494cf72a8d1081945c5bd23c68ffe70cd) feat: enable controller HA (#602)
 * [d2e0513c](https://github.com/numaproj/numaflow/commit/d2e0513c5bb3358649da42b1babff4ee46c9e8a6) feat: expose dnspolicy and dnsconfig to pod template (#598)
 * [6002c829](https://github.com/numaproj/numaflow/commit/6002c829f4600ef67d72fdda0e4693364dccfc07) Chore: tickgen changes to test reduce pipelines (#587)
 * [b1aee945](https://github.com/numaproj/numaflow/commit/b1aee94533594cbee6470257d21a3d13c840ce70) feat: bidirectional streaming (#553)
 * [67fc688a](https://github.com/numaproj/numaflow/commit/67fc688ab8be984d48182d3a0f4b9b9b99a8c2c8) feat: use customized binary serde for nats message payload (#585)
 * [cc104199](https://github.com/numaproj/numaflow/commit/cc1041995d13ed899b3fce8fca955541cebf4588) fix: Idle watermark fix for read batch size > 0 and partial idle outgoing edges (#575)
 * [df1574da](https://github.com/numaproj/numaflow/commit/df1574dad468cbc68eadb75b7a61c930d6f31578) feat: implement watermark propagation for source data transformer (#557)
 * [45d5c396](https://github.com/numaproj/numaflow/commit/45d5c396d2f369f0a9724704d91a7e07aa64895f) feat: namespace search (#559)
 * [b14d470f](https://github.com/numaproj/numaflow/commit/b14d470fa2b8bcd746f7e75b4ec20453297d53e6) fix: refine log for buffer validation. Fixes #185 (#573)
 * [a8e8bb15](https://github.com/numaproj/numaflow/commit/a8e8bb1580dcd80e2a3625a2dc6c0a7c95a823ef) feat: add readiness and liveness check for daemon server. Fixes #543 (#571)
 * [fd6acb6d](https://github.com/numaproj/numaflow/commit/fd6acb6da5b63c743225df3ea3743935f211aaba) feat: marshal/unmarshal binary for read message (#565)
 * [d1032b4c](https://github.com/numaproj/numaflow/commit/d1032b4ce08a509223b5a7dbce4570b54d9e90a5) chore(deps): bump golang.org/x/net from 0.0.0-20220722155237-a158d28d115b to 0.7.0 (#568)
 * [fd00ebdf](https://github.com/numaproj/numaflow/commit/fd00ebdf938933bc2b735e03a7df53bedf1f48d7) chore(deps): bump golang.org/x/text from 0.3.7 to 0.3.8 (#567)
 * [05ec77f5](https://github.com/numaproj/numaflow/commit/05ec77f5d839dd13d6c1ef1cded94178f01d45e6) feat: expose image pull policy to user defined containers (#563)
 * [ecbe3a00](https://github.com/numaproj/numaflow/commit/ecbe3a0061e02b05077f8a29649b692802a986a5) fix: typos in reduce examples (#556)
 * [0dc3f5c6](https://github.com/numaproj/numaflow/commit/0dc3f5c6d6e99c96937c5ee3a46f3f7e2723363f) feat: edge-watermark (#537)
 * [77298c85](https://github.com/numaproj/numaflow/commit/77298c853e0184f554bf22b13858cba0e35ed922) feat: enable envFrom for user defined containers (#554)

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

 * [92925c15](https://github.com/numaproj/numaflow/commit/92925c15485a802600c5cb54cf603f2c1ceae027) Update manifests to v0.7.1
 * [2f8e147a](https://github.com/numaproj/numaflow/commit/2f8e147a3106bead03655584a41e9951c7c17950) feat: remove secret watch privilege dependency (#542)
 * [f8e7daae](https://github.com/numaproj/numaflow/commit/f8e7daae4f11b8c6a96676dd11b95d13efa830b2) fix: Use a copied object to update (#541)
 * [98de2459](https://github.com/numaproj/numaflow/commit/98de2459b9a2ed733fa2a0c3be804a7f8241156f) chore(deps): bump github.com/emicklei/go-restful from 2.9.5+incompatible to 2.16.0+incompatible (#539)
 * [0df812c1](https://github.com/numaproj/numaflow/commit/0df812c1c950f76a825847ea4b9c61d836102c38) feat: improve reduce performance (#501)
 * [ab49de68](https://github.com/numaproj/numaflow/commit/ab49de684aacbf5876da956dbad62d92b1ffa6ac) feat: Offset time idle watermark put (#529)
 * [c0aa7c1e](https://github.com/numaproj/numaflow/commit/c0aa7c1e3769a8a9180e0a4b1a0f84facf393046) fix: securityContext not applied to container templates (#528)
 * [ac33fb02](https://github.com/numaproj/numaflow/commit/ac33fb0266f7ddc8be04f466c75c370f2a4e90cc) feat: idle watermark v0 (#520)
 * [2844cfb6](https://github.com/numaproj/numaflow/commit/2844cfb60480a9711f6e5ea4f233cf3fa37e9e9b) feat: Reduce UI Support (#500)
 * [e701180d](https://github.com/numaproj/numaflow/commit/e701180df3c218894a65e2b75de2f3811c21dd40) feat: enable RuntimeClassName for vertex pod (#519)
 * [bb94f631](https://github.com/numaproj/numaflow/commit/bb94f6318e32d9e0ae1cf4fd494ea06334cf1a03) feat: add builtin filter and event time extractor for source transformer (#517)
 * [4562196d](https://github.com/numaproj/numaflow/commit/4562196d00bf4db7d1c3d43bbcfcd4d699f864f1) chore(deps): bump ua-parser-js from 0.7.32 to 0.7.33 in /ui (#507)
 * [764cefda](https://github.com/numaproj/numaflow/commit/764cefdaa52465aa936283a5c6574c4757a79f78) Add an e2e test for source data transformer (#505)
 * [7665d6ce](https://github.com/numaproj/numaflow/commit/7665d6cef3cc8330bfe5b826d6bbfbd57e240568) feat: Implement source data transformer and apply to all existing sources (#487)
 * [d0226084](https://github.com/numaproj/numaflow/commit/d0226084b020d35f618669cb56481badd07e8f38) fix: -ve metrics and return early if isLate (#495)

### Contributors

 * Derek Wang
 * Juanlu Yu
 * Keran Yang
 * Vedant Gupta
 * Vigith Maurice
 * ashwinidulams
 * dependabot[bot]

## v0.7.0 (2023-01-13)

 * [734e5d3b](https://github.com/numaproj/numaflow/commit/734e5d3b44dee2ef690c9a1fe4d9d1ecb092a16c) Update manifests to v0.7.0
 * [5d6c5336](https://github.com/numaproj/numaflow/commit/5d6c53369a6ea8da5f9f5036fce9aa81f6308fbf) fix: JetStream context KV store/watch fix (#460)
 * [d6152e77](https://github.com/numaproj/numaflow/commit/d6152e772a03e646f2841a34497d498e4c2234c3) doc: reduce persistent store (#458)
 * [ac77656d](https://github.com/numaproj/numaflow/commit/ac77656dec8164fc162b8339bfff8138d17f73b0) doc: reduce documentation (#448)
 * [257356af](https://github.com/numaproj/numaflow/commit/257356af0c932a9c7e84573c9f163e37a3e06dc4) chore(deps): bump json5 from 1.0.1 to 1.0.2 in /ui (#454)
 * [7752db4b](https://github.com/numaproj/numaflow/commit/7752db4b7d4233e2a691c0d1cc9ef2348dc75ab5) refactor: simplify http request construction in test cases (#444)
 * [1a10af4c](https://github.com/numaproj/numaflow/commit/1a10af4c20f051e46c063f9d946a39c208b6ec60) refactor: use exact matching instead of regex to perform e2e data validation. (#443)
 * [2777e27a](https://github.com/numaproj/numaflow/commit/2777e27ac0cdfcc954bf5e453b92b7f4e8a5c201) doc: windowing fixed and sliding (#439)
 * [70fc008f](https://github.com/numaproj/numaflow/commit/70fc008ffb0016a7310612d7cac191920207d0a6) refactor: move redis sink resources creation to E2ESuite (#437)
 * [6c078b42](https://github.com/numaproj/numaflow/commit/6c078b42046b4733f702b3fbb585578d6304dafb) refactor: a prototype for enhancing E2E test framework (#424)
 * [e7021c9a](https://github.com/numaproj/numaflow/commit/e7021c9ae668724c11ac81fb49527ae8ce0f9240) feat: pipeline watermark (#416)

### Contributors

 * Derek Wang
 * Juanlu Yu
 * Keran Yang
 * Vedant Gupta
 * Vigith Maurice
 * dependabot[bot]

## v0.7.0-rc1 (2022-12-16)

 * [71887db5](https://github.com/numaproj/numaflow/commit/71887db5cce231b9b0a236f8f00ddeb0d40ac01a) Update manifests to v0.7.0-rc1
 * [dda4835d](https://github.com/numaproj/numaflow/commit/dda4835d87993dffba16b5e8a2a9e4b6b0e6cdba) feat: reduce metrics. Closes #313 (#414)
 * [85dbe4d7](https://github.com/numaproj/numaflow/commit/85dbe4d7f43433ad2a17531f053dc91ee829835c) feat: udsink grpc stream (#421)
 * [fa07587f](https://github.com/numaproj/numaflow/commit/fa07587f3a032a49e73827c4f069480add8eceb9) chore(doc): scope UDF under a dir (#426)
 * [0a911da9](https://github.com/numaproj/numaflow/commit/0a911da927c1cb61943430fad8edef2f3b1f661b) feat: sliding window. closes #339 (#354)
 * [a46fb964](https://github.com/numaproj/numaflow/commit/a46fb9647c990ab69b0071bfc0ea6dfd6019f1bf) refactor: nats/jetstream testing (#418)
 * [13d95c48](https://github.com/numaproj/numaflow/commit/13d95c487b8ea2ef8b0897557745e4a9c825ee1d) feat: nats as source (#411)
 * [f1e7c737](https://github.com/numaproj/numaflow/commit/f1e7c73732f064596f2543559b8101262f72f61d) fix: adding lock while discovering partitions, Closes #412 (#413)
 * [3b64d674](https://github.com/numaproj/numaflow/commit/3b64d674f049907bcfb9b9558e45cca80f21f915) fix(test): e2e-api-pod can not start on M1 mac (#410)
 * [6504a562](https://github.com/numaproj/numaflow/commit/6504a5620481208c214048f89f4f01f918f5586c) fix: getWatermark to return-1 if any processor returns -1  (#402)
 * [d4d22041](https://github.com/numaproj/numaflow/commit/d4d22041d5438d31e5106c708974d2bfebff8e96) fix: e2e testing for PBQ WAL with reduce pipeline (#393)
 * [80e97850](https://github.com/numaproj/numaflow/commit/80e978503d3b5d2db36c231da8f2fda5cd4ccc8e) feat: add Grafana instruction and a dashboard template. Closes #287 (#381)
 * [2f94a915](https://github.com/numaproj/numaflow/commit/2f94a915be7627a6ef3349f5e47b30f47dd63561) fix: unit tests for replay. Closes #373 (#377)
 * [8f367ab2](https://github.com/numaproj/numaflow/commit/8f367ab2e0a02b7d5c8c7654bf16a34023dadc96) chore(docs): update docs (#380)
 * [efe4d41c](https://github.com/numaproj/numaflow/commit/efe4d41ccd56dea30753c477b42004301ed3209a) fix: best effort processing during SIGTERM. Closes #371 (#372)
 * [7e041d87](https://github.com/numaproj/numaflow/commit/7e041d87308493bc21e1c317ed9e79c6ada2b725) feat(wal): First pass to implement WAL and hook to PBQ store. (#344)
 * [256e66b3](https://github.com/numaproj/numaflow/commit/256e66b326ae5a0c9959758f18ad9bc07c40fd65) feat: watermark otwatcher enhancement (#364)
 * [f8170577](https://github.com/numaproj/numaflow/commit/f8170577eada6972e64f5d338b9d387f23111e47) refactor(docs): group docs in categories (#362)
 * [1a5d424f](https://github.com/numaproj/numaflow/commit/1a5d424f8a1d93a681d5897eb1bcceee4e851bb2) chore(deps): bump loader-utils from 2.0.3 to 2.0.4 in /ui (#356)
 * [6c8f03f2](https://github.com/numaproj/numaflow/commit/6c8f03f28024670a29713c202c439df3688bca0a) fix(controller): vertex nil check for edge listing. Fixes #352 (#353)
 * [f254c28a](https://github.com/numaproj/numaflow/commit/f254c28a20f5a1e6f70a3f78e9874f47eca39515) fix: data race in pbq manager. Closes #348 (#349)
 * [bc359457](https://github.com/numaproj/numaflow/commit/bc359457ce221b18c46d93cc0e987f2058d59756) Chore: Windower interface. closes #234 (#340)
 * [3206bd12](https://github.com/numaproj/numaflow/commit/3206bd1282a89a8b3e760ff627a6fb4fd5dbba0d) feat: add minikube, kind and podman support (#206)
 * [d40ecdaa](https://github.com/numaproj/numaflow/commit/d40ecdaa8deed95c22611c3086e9b8175fdc44f0) refactor: Close watermark fetcher and publisher correctly (#336)
 * [0d8f659e](https://github.com/numaproj/numaflow/commit/0d8f659e3d83eec1b07420439299134361fe58b2) passing window information inside the context (#341)
 * [89516277](https://github.com/numaproj/numaflow/commit/89516277919a435fa5fce837cd712f734c0cae7e) feat: timestamp in UI to display milliseconds. closes #280 (#337)
 * [5c43f5aa](https://github.com/numaproj/numaflow/commit/5c43f5aaea990fe9a13b249420b29c365c1a8ce2) Simple reduce pipeline. Fixes #289 (#317)
 * [7f5d86c3](https://github.com/numaproj/numaflow/commit/7f5d86c3021e952a84c0796e8a71e970633b981c) feat: add blackhole sink. Closes #329 (#330)
 * [10f355c3](https://github.com/numaproj/numaflow/commit/10f355c3c536a07c1e4d3cff9d27dd6101f361de) fix: move watermark based on the head of the read batch (#332)
 * [b2b975f3](https://github.com/numaproj/numaflow/commit/b2b975f3ad1bb7d9b28c7a3b4783d620a37850f2) feat: configurable jetstream storage (#328)
 * [3fcf637c](https://github.com/numaproj/numaflow/commit/3fcf637cebd66c0c0224a4da734191b9ad97e625) feat: support adding sidecars in vertex pods. Closes #323 (#325)
 * [6eab1b5b](https://github.com/numaproj/numaflow/commit/6eab1b5bd334146316856bed473b2c3def4bb8eb) feat: populate watermark settings to vertex spec. Closes #320 (#321)
 * [2355978b](https://github.com/numaproj/numaflow/commit/2355978b1ffd899cb6c70b140f1428754fc5226c) doc: add few use cases (#318)
 * [bfc1eb60](https://github.com/numaproj/numaflow/commit/bfc1eb60482ed4d3ca9c809f7aa1786a64ec487d) Chore: run in sdks-e2e tests, python-udsink log check before go-udsink (#315)
 * [cda41eca](https://github.com/numaproj/numaflow/commit/cda41eca93e3537fa57bfa8e58c7de2579659424) fix: jetstream build watermark progressors bug (#316)
 * [bfab8f1d](https://github.com/numaproj/numaflow/commit/bfab8f1d0c0ef9fe6ace7c6b365318bc7687ab0f) feat: update watermark offset bucket implementation (#307)
 * [1d86aa5f](https://github.com/numaproj/numaflow/commit/1d86aa5f04b27aab132b37fa233d8fdfb81fccad) feat: shuffling support (#306)
 * [b817920a](https://github.com/numaproj/numaflow/commit/b817920ab31630f43689ea73f7e5b43a0965a5f8) feat: customize init-container resources. Closes #303 (#304)
 * [0548d4d3](https://github.com/numaproj/numaflow/commit/0548d4d3b4c12964b33eab70f882867d51397241) feat: watermark - remove non-share OT bucket option (#302)
 * [cc44875b](https://github.com/numaproj/numaflow/commit/cc44875b2a2e01694ceca1fb085c3423bd330a38) feat: customization for batch jobs. Closes #259 (#300)
 * [d16015f3](https://github.com/numaproj/numaflow/commit/d16015f34d66c07149bff240b3890357fad2d436) refactor: abstract pod template (#296)
 * [4550f459](https://github.com/numaproj/numaflow/commit/4550f45917567cb6909c4800f40610c352c7c330) feat: customization for daemon deployment. Closes #223 (#290)
 * [d61377a5](https://github.com/numaproj/numaflow/commit/d61377a52ce4fc1f7f8c5686f84f08464aca2f12) feat: add pvc support for reduce vertex PBQ (#292)
 * [b0e3f944](https://github.com/numaproj/numaflow/commit/b0e3f944c19f20eaebb28b3e58f40cafcf9e31f7) fix(doc): hyperlink for security doc (#288)
 * [6c61728d](https://github.com/numaproj/numaflow/commit/6c61728d8fb1eeb657ada7b7550d94ff13a51812) feat: support adding init containers to vertices. Closes #284 (#285)
 * [88cf272c](https://github.com/numaproj/numaflow/commit/88cf272c49f5232e1f78fc12095d634b33940d3f) fix: retry when getting EOF error at E2E test (#281)
 * [1436071c](https://github.com/numaproj/numaflow/commit/1436071c8688a51a10daecb9c972658a9cb30cfd) feat: Watermark millisecond. Fixes #201 (#278)
 * [7a7e7945](https://github.com/numaproj/numaflow/commit/7a7e7945eef86b29703df23021203f5b5132f274) feat: add pipeline node counts and age to printcolumn. Closes #267 (#282)
 * [5883e973](https://github.com/numaproj/numaflow/commit/5883e973d368e7f457acfd88b6ea27219e96694f) feat: introduce reduce UDF. Closes #246 (#262)
 * [a0dc17f8](https://github.com/numaproj/numaflow/commit/a0dc17f8212d34f2bc7dd5aa2bc91a3454381d64) feat: add pandoc to required tools development doc. Closes #276 (#277)
 * [284be2d6](https://github.com/numaproj/numaflow/commit/284be2d692c3c64dc104d3587afedc3d4473d37b) feat: add isbsvc type and age to printcolumn. Closes #268 (#275)
 * [7bb689bc](https://github.com/numaproj/numaflow/commit/7bb689bc00aae21c3e77dceb6254b55040c60324) fix: watermark consumer fix (#273)
 * [8ff9e28e](https://github.com/numaproj/numaflow/commit/8ff9e28e4071749b14c4ab8d9043d2b97da94714) refactor: generalize watermark fetching as an interface of ISB service. Fixes #252 (#263)
 * [8e038d1e](https://github.com/numaproj/numaflow/commit/8e038d1ee301936cdcf2d7f4355199ba641725c4) fix: set default property values for minimal CRD installation (#264)
 * [57df392f](https://github.com/numaproj/numaflow/commit/57df392f227eab168d36907aa5f4099274c29f48) fix: validate only one isbsvc implementation type is defined. Fixes #269 (#271)
 * [21378a36](https://github.com/numaproj/numaflow/commit/21378a3615184395bda26b05d0a2be10f104dcc8) fix: main branch make build failure: math.MaxInt64 for int type (#265)
 * [3d9997d6](https://github.com/numaproj/numaflow/commit/3d9997d6582b6fca215631cb611cbeb2eebaf7a4) fix: nil pointer deref when running example with minimal CRD. Fixes #260 (#261)
 * [4b0cbc37](https://github.com/numaproj/numaflow/commit/4b0cbc376f09380ce193ed42550589e17d964936) fix: retry only the failed offsets (#255)
 * [27e6a875](https://github.com/numaproj/numaflow/commit/27e6a8755bd1092b31d93fac891d1decd006a093) fix: re-enable build constraint on processor manager test. Fixes #256 (#257)
 * [98b3ec4d](https://github.com/numaproj/numaflow/commit/98b3ec4d82315a62512c5750610fad7f73f17880) fix: container resource for jetstream isbsvc. Fixes #253 (#254)
 * [e615e16e](https://github.com/numaproj/numaflow/commit/e615e16edf05f2cd32c75ef43d2d3a4d1a6bd541) fix: update vertex watermark fetching logic. Fixes: #134 (#245)
 * [30c734bd](https://github.com/numaproj/numaflow/commit/30c734bd0f2b3698e5c86e6d41ce33164edbc69d) fix: watermark watcher leak (#242)
 * [3d29f79d](https://github.com/numaproj/numaflow/commit/3d29f79d5dda7320048bff5662064d2a449ed6f9) fix(docs): fix a typo (#241)
 * [0370fd6c](https://github.com/numaproj/numaflow/commit/0370fd6c91ef68c6300fbfba54f537b0742124a9) feat: Support running UX server with namespace scope. Fixes #248 (#249)
 * [29f15d57](https://github.com/numaproj/numaflow/commit/29f15d57a125c54b6de8c4cd29d16e3d8473b655) fix(manifests): Include ServiceAccount in namespace scoped install (#240)
 * [6870d2a4](https://github.com/numaproj/numaflow/commit/6870d2a4558107cf824e5ba91ff8c65125a43eed) fix: Watermark close fix and removed the nil check (#238)
 * [998e3988](https://github.com/numaproj/numaflow/commit/998e3988c38c37aab252cce3176b24999f54ab97) fix: skip publishing watermarks to unexpected vertices. Fixes #235 (#236)
 * [fff05f32](https://github.com/numaproj/numaflow/commit/fff05f32a137ed949718a6cfbb2afa5d0dac4b5d) fix: update default watermark to -1. Fixes #133 (#218)
 * [a23e3592](https://github.com/numaproj/numaflow/commit/a23e35920598f4735eb578c8898fe4fe57f02d07) feat: support disabling TLS and changing port for UX server (#228)
 * [5a4387c7](https://github.com/numaproj/numaflow/commit/5a4387c7ce55947842dd0f58a06769a986a7f885) feat: reducer for stream aggregation without fault tolerance (#208)
 * [fc2ba4e9](https://github.com/numaproj/numaflow/commit/fc2ba4e97848578aaa48e62f87bb208574b05cf3) feat: in-memory watermark store for better testing (#216)
 * [c89aef31](https://github.com/numaproj/numaflow/commit/c89aef31286be5f0a7ac0fce205e389410ba12e6) Add USERS.md (#221)
 * [2377c4c6](https://github.com/numaproj/numaflow/commit/2377c4c69b2255b4e3005f562eb2f1b8161f7b55) fix(watermark): generator should not publish wm for every message (#217)

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

 * [845c9594](https://github.com/numaproj/numaflow/commit/845c9594e026dbaa22f139cd20a9637236d95deb) Update manifests to v0.6.5
 * [676ea1c6](https://github.com/numaproj/numaflow/commit/676ea1c603a9d49f24449e27db91367c894b2a08) fix: adding lock while discovering partitions, Closes #412 (#413)
 * [c439a6a1](https://github.com/numaproj/numaflow/commit/c439a6a1851101e60b84bd23d323320abbc5fac8) fix(test): e2e-api-pod can not start on M1 mac (#410)
 * [115a69d6](https://github.com/numaproj/numaflow/commit/115a69d69d82221e924f86c36af9f88ac49dc108) fix: getWatermark to return-1 if any processor returns -1  (#402)
 * [e6e24eef](https://github.com/numaproj/numaflow/commit/e6e24eef6c0ba8725ad87d24bcaf2cb427784485) fix: e2e testing for PBQ WAL with reduce pipeline (#393)
 * [7ef3d47c](https://github.com/numaproj/numaflow/commit/7ef3d47c8cb113fa6c1fb46186b8e74874358a00) feat: add Grafana instruction and a dashboard template. Closes #287 (#381)
 * [13ce4d27](https://github.com/numaproj/numaflow/commit/13ce4d279e93b1cbcc82f768b29232b7a3d82a67) fix: unit tests for replay. Closes #373 (#377)

### Contributors

 * Derek Wang
 * Keran Yang
 * Yashash H L
 * xdevxy

## v0.6.4 (2022-11-28)

 * [ad9719a6](https://github.com/numaproj/numaflow/commit/ad9719a61ec7208da36a228cea129f65cdf70d77) Update manifests to v0.6.4
 * [c5e82176](https://github.com/numaproj/numaflow/commit/c5e82176b1dd70aa991797bffaaf02bc4a8f6609) chore(docs): update docs (#380)
 * [1b244c1a](https://github.com/numaproj/numaflow/commit/1b244c1a0eea48ab9c75a0704cf70848640e5d6b) fix: best effort processing during SIGTERM. Closes #371 (#372)
 * [9bb8ebd5](https://github.com/numaproj/numaflow/commit/9bb8ebd58835618d62402f11a191063cb299170c) feat(wal): First pass to implement WAL and hook to PBQ store. (#344)

### Contributors

 * Derek Wang
 * Vigith Maurice
 * xdevxy

## v0.6.3 (2022-11-18)

 * [3cf391b1](https://github.com/numaproj/numaflow/commit/3cf391b19e862744ae04ef350e34dc57d88fe6b1) Update manifests to v0.6.3
 * [bec020b7](https://github.com/numaproj/numaflow/commit/bec020b78f7c1aa5cec4e0c6503beadb71955465) feat: watermark otwatcher enhancement (#364)
 * [2b5478fc](https://github.com/numaproj/numaflow/commit/2b5478fca9cd5781593750c82ec29d3cd0a65b85) refactor(docs): group docs in categories (#362)
 * [6d9e129b](https://github.com/numaproj/numaflow/commit/6d9e129b5fa374c0405d4fce18d22eb351acc488) chore(deps): bump loader-utils from 2.0.3 to 2.0.4 in /ui (#356)
 * [77364a4d](https://github.com/numaproj/numaflow/commit/77364a4df0114f2d8b296701a6b9e85f2f1041a7) fix(controller): vertex nil check for edge listing. Fixes #352 (#353)
 * [7db4fe56](https://github.com/numaproj/numaflow/commit/7db4fe562bd0e9a1034c7d02345251089827e20b) fix: data race in pbq manager. Closes #348 (#349)
 * [6f5e83a7](https://github.com/numaproj/numaflow/commit/6f5e83a70a2b97ec87bfb416abe83490393eb179) Chore: Windower interface. closes #234 (#340)
 * [24ba5157](https://github.com/numaproj/numaflow/commit/24ba5157085da3c55d3a0cde617e2ccfc8b7346e) feat: add minikube, kind and podman support (#206)
 * [12c6ca52](https://github.com/numaproj/numaflow/commit/12c6ca527d81d14fd3f9c7b20dbe3bf2795cc08e) refactor: Close watermark fetcher and publisher correctly (#336)
 * [2a8b97e1](https://github.com/numaproj/numaflow/commit/2a8b97e1551f5f2c6e1c447ec7b23f10a64801c3) passing window information inside the context (#341)
 * [d52a5a75](https://github.com/numaproj/numaflow/commit/d52a5a75d5eb73fe306ea62f387e00ad7e1f7acf) feat: timestamp in UI to display milliseconds. closes #280 (#337)
 * [de9059cf](https://github.com/numaproj/numaflow/commit/de9059cf3884d3fe2add9ec7dd2164509400ea0c) Simple reduce pipeline. Fixes #289 (#317)
 * [3d936a50](https://github.com/numaproj/numaflow/commit/3d936a5061f1ad1542c9429f8cd2351dee469a99) feat: add blackhole sink. Closes #329 (#330)
 * [45905475](https://github.com/numaproj/numaflow/commit/45905475dd0da7dfa29c75c905a33a49f9f47e73) fix: move watermark based on the head of the read batch (#332)
 * [049e5c66](https://github.com/numaproj/numaflow/commit/049e5c66bd95d15fa79066ee66edd222b6af8b1d) feat: configurable jetstream storage (#328)
 * [ee5cd642](https://github.com/numaproj/numaflow/commit/ee5cd6425b1ec12f37332159fcdf06b12d38907b) feat: support adding sidecars in vertex pods. Closes #323 (#325)

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

 * [99be6c08](https://github.com/numaproj/numaflow/commit/99be6c088a8dee0ae7ff74a00fc991f4009beaa7) Update manifests to v0.6.2
 * [dc733da1](https://github.com/numaproj/numaflow/commit/dc733da118b2683c4f3359763b9252d7fa11785a) feat: populate watermark settings to vertex spec. Closes #320 (#321)
 * [2b247cad](https://github.com/numaproj/numaflow/commit/2b247cad0e560accf14467407b7aada27610f7bc) doc: add few use cases (#318)
 * [07ffa168](https://github.com/numaproj/numaflow/commit/07ffa168f49210d86c11cc982e5018e9a4afb5e2) Chore: run in sdks-e2e tests, python-udsink log check before go-udsink (#315)
 * [7b3285b9](https://github.com/numaproj/numaflow/commit/7b3285b956bc97e892ad4107f5808e2aa3a9fca6) fix: jetstream build watermark progressors bug (#316)
 * [1198a609](https://github.com/numaproj/numaflow/commit/1198a6097f97817d7462b741cdc789c625424ffe) feat: update watermark offset bucket implementation (#307)
 * [34a6d709](https://github.com/numaproj/numaflow/commit/34a6d7095229509deff1503b6d5fd62b3a2cf93f) feat: shuffling support (#306)
 * [448127ff](https://github.com/numaproj/numaflow/commit/448127ff0c47db9f5afd502d5df846a44d536be1) feat: customize init-container resources. Closes #303 (#304)
 * [61cf2272](https://github.com/numaproj/numaflow/commit/61cf22723a43b5c48df9c1dea3e39cefd6481182) feat: watermark - remove non-share OT bucket option (#302)
 * [51c9ff42](https://github.com/numaproj/numaflow/commit/51c9ff42ba520c2b39e5568c97810e98177a56cb) feat: customization for batch jobs. Closes #259 (#300)
 * [afbe2557](https://github.com/numaproj/numaflow/commit/afbe2557691e41a2487591c9b732814494544e12) refactor: abstract pod template (#296)
 * [24089460](https://github.com/numaproj/numaflow/commit/240894606a5e5968a388ff4c3c8da29e187f73cf) feat: customization for daemon deployment. Closes #223 (#290)
 * [bedf567c](https://github.com/numaproj/numaflow/commit/bedf567cf17573421be6d2f2c19e54941c33fe97) feat: add pvc support for reduce vertex PBQ (#292)
 * [2341614b](https://github.com/numaproj/numaflow/commit/2341614b2e9ad5fbc9ba9d1a76531bf5e84250d6) fix(doc): hyperlink for security doc (#288)
 * [6c05190d](https://github.com/numaproj/numaflow/commit/6c05190d31167a0d1cbf938633ee2225512b5de1) feat: support adding init containers to vertices. Closes #284 (#285)
 * [dc96b872](https://github.com/numaproj/numaflow/commit/dc96b8720d009e985f016c1eff45ec465fe22d7a) fix: retry when getting EOF error at E2E test (#281)
 * [f5db937c](https://github.com/numaproj/numaflow/commit/f5db937ccd4c5f6a2d873c24b1c78b314dc35046) feat: Watermark millisecond. Fixes #201 (#278)
 * [c1535365](https://github.com/numaproj/numaflow/commit/c153536500f3983016ffd91883cd837f34645679) feat: add pipeline node counts and age to printcolumn. Closes #267 (#282)

### Contributors

 * David Seapy
 * Derek Wang
 * Juanlu Yu
 * Keran Yang
 * Vigith Maurice

## v0.6.1 (2022-10-26)

 * [32b284f6](https://github.com/numaproj/numaflow/commit/32b284f626aaffdfc16c267a3890e41cdc5f0142) Update manifests to v0.6.1
 * [9684e161](https://github.com/numaproj/numaflow/commit/9684e1616e97ae4eeb791d7a9164d2f31e9317a4) fix(manifests): Include ServiceAccount in namespace scoped install (#240)
 * [fe83918a](https://github.com/numaproj/numaflow/commit/fe83918a458d632af71daccff56bf7d00aaaa012) fix(docs): fix a typo (#241)
 * [f2094b4b](https://github.com/numaproj/numaflow/commit/f2094b4baa031f985068f5accf9426a599b72f97) feat: introduce reduce UDF. Closes #246 (#262)
 * [e19a1e7d](https://github.com/numaproj/numaflow/commit/e19a1e7d90644378201eb8854b226b5c50c6cf9c) feat: add pandoc to required tools development doc. Closes #276 (#277)
 * [9a937118](https://github.com/numaproj/numaflow/commit/9a937118fd6ffcb5ff19803423019ede70e20d4b) feat: add isbsvc type and age to printcolumn. Closes #268 (#275)
 * [f25e303e](https://github.com/numaproj/numaflow/commit/f25e303e773d4a0e1f3d32815f839628e35278f0) fix: watermark consumer fix (#273)
 * [d2a3d908](https://github.com/numaproj/numaflow/commit/d2a3d90823046b103b9729b142903ba9f1903bf4) refactor: generalize watermark fetching as an interface of ISB service. Fixes #252 (#263)
 * [5ffcadcc](https://github.com/numaproj/numaflow/commit/5ffcadccafeef5711f784acb005c51051c06fd18) fix: set default property values for minimal CRD installation (#264)
 * [17a99564](https://github.com/numaproj/numaflow/commit/17a99564587bf4cc68d057bccd808ea611b1bf7d) fix: validate only one isbsvc implementation type is defined. Fixes #269 (#271)
 * [2272a1fc](https://github.com/numaproj/numaflow/commit/2272a1fcbb8c0fdb676093eba1b1e27e13fef257) fix: main branch make build failure: math.MaxInt64 for int type (#265)
 * [02c31d27](https://github.com/numaproj/numaflow/commit/02c31d277b0a74f6e97aefd88c2e11f32d7b4f95) fix: nil pointer deref when running example with minimal CRD. Fixes #260 (#261)
 * [391b53e1](https://github.com/numaproj/numaflow/commit/391b53e1203d0989f71bcad4840446e0dda55324) fix: retry only the failed offsets (#255)
 * [7b42dc80](https://github.com/numaproj/numaflow/commit/7b42dc80c813c694ba494cb6f6b86347745e5b7b) fix: re-enable build constraint on processor manager test. Fixes #256 (#257)
 * [34360490](https://github.com/numaproj/numaflow/commit/3436049011b6ed245b048ec81a9185cde1e48e62) fix: container resource for jetstream isbsvc. Fixes #253 (#254)
 * [33ce7422](https://github.com/numaproj/numaflow/commit/33ce74222a76dc8deeb1306da2165d90571fdba1) fix: update vertex watermark fetching logic. Fixes: #134 (#245)
 * [fd219a5c](https://github.com/numaproj/numaflow/commit/fd219a5cda3623549abf47e064a4549470056b59) fix: watermark watcher leak (#242)
 * [979a3a3f](https://github.com/numaproj/numaflow/commit/979a3a3f8d0649cb6ac82722513a3c96827bfb70) feat: Support running UX server with namespace scope. Fixes #248 (#249)
 * [5e9d1c1c](https://github.com/numaproj/numaflow/commit/5e9d1c1ceb88b3ad9cab2a947018e8935cbbd73f) fix: Watermark close fix and removed the nil check (#238)
 * [340bd820](https://github.com/numaproj/numaflow/commit/340bd820ddd25d507541dcd368ce1eaf51ecc9e3) fix: skip publishing watermarks to unexpected vertices. Fixes #235 (#236)
 * [904b2cde](https://github.com/numaproj/numaflow/commit/904b2cde562351fd39cc54b6ac0c91baa9ab3047) fix: update default watermark to -1. Fixes #133 (#218)
 * [321e285f](https://github.com/numaproj/numaflow/commit/321e285fa437a0fbf33cb127ce6abbe1feaf0159) feat: support disabling TLS and changing port for UX server (#228)
 * [d0d74e19](https://github.com/numaproj/numaflow/commit/d0d74e19745084dbb677a56d04205ddff435d427) feat: reducer for stream aggregation without fault tolerance (#208)
 * [06a9b58a](https://github.com/numaproj/numaflow/commit/06a9b58a3e3b8452e767f2cc34229b5ee0145aad) feat: in-memory watermark store for better testing (#216)
 * [f25cc58e](https://github.com/numaproj/numaflow/commit/f25cc58e6b9fc504c3b6a15cbd503f479f60df1d) Add USERS.md (#221)
 * [a37cece9](https://github.com/numaproj/numaflow/commit/a37cece931629f230ad9c981351641c23dfdb3f0) fix(watermark): generator should not publish wm for every message (#217)

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

 * [48aad5fc](https://github.com/numaproj/numaflow/commit/48aad5fcbf855380b06f90c082e92571916e4c54) Update manifests to v0.6.0
 * [09ce54f1](https://github.com/numaproj/numaflow/commit/09ce54f1008d5a822045b53779b1722aa503700f) fix(autoscaling): Ack pending should be included in total pending calculation (#212)
 * [9922787c](https://github.com/numaproj/numaflow/commit/9922787ce1d00b97ac119081a303ae26d8281cb8) fix(autoscaling): Skip autoscaling if vertex is not in running phase (#207)
 * [bc2380a7](https://github.com/numaproj/numaflow/commit/bc2380a7b6a035f14fbffe0a0cbfe613056e6b93) feat: ISBSVC add support for redis cluster mode (#195)
 * [72a96a58](https://github.com/numaproj/numaflow/commit/72a96a5843bc7dbcde4a092cbfc8e771d0e70bef) refactor: move controllers package to pkg/reconciler (#192)
 * [b1b78faa](https://github.com/numaproj/numaflow/commit/b1b78faafd0102f6c19d4905b60be4f4d97153ad) fix: update udf fetchWatermark and publishWatermark initial values (#193)
 * [d4912600](https://github.com/numaproj/numaflow/commit/d491260060ae25d57b42b7df76781b34437cf355) fix(docs): readme for UI development (#181)
 * [6b121c6e](https://github.com/numaproj/numaflow/commit/6b121c6e4f87acaea2a7828511c187f5508ea62a) feat: grpc udsink (#174)
 * [567da7b0](https://github.com/numaproj/numaflow/commit/567da7b0d0235a171f7c7b3bdefb5b8b1ca5acb3) fix: numaflow-go udf example & docs (#177)
 * [4652f808](https://github.com/numaproj/numaflow/commit/4652f808a35b73c38f22d8d03df46405959198cd) fix: use scale.max if it is set (#179)
 * [900314bc](https://github.com/numaproj/numaflow/commit/900314bc6c01e647b53c4fa916fd65dfd0ded221) fix broken path (#176)
 * [3b02f2a6](https://github.com/numaproj/numaflow/commit/3b02f2a6a0e1a0e8bd03479eadb0f713a4de09fb) feat: Shuffle implementation (#169)
 * [021bb9df](https://github.com/numaproj/numaflow/commit/021bb9dfdb4a3d8da44275397f06d022b0edcfc4) feat: windowing operations (#157)
 * [7d411294](https://github.com/numaproj/numaflow/commit/7d411294a28cd9aa135efe3de14405ebc637e73c) feat: watermark for sources (#159)
 * [5f5b2dfd](https://github.com/numaproj/numaflow/commit/5f5b2dfdc4d999b65d4a3b7a5366e3234677bb61) fix: daemon service client memory leak (#161)
 * [bfe96695](https://github.com/numaproj/numaflow/commit/bfe966956cc9ea36759c62f5a787c0e1ed98fb68) pbq implementation (#155)
 * [8dfedd83](https://github.com/numaproj/numaflow/commit/8dfedd838794d2bee94b18f720057ef9e99b73e0) feat: check if udf is running in liveness probe  (#156)
 * [81e76d82](https://github.com/numaproj/numaflow/commit/81e76d82e375c4f5e9f392cf53a2624fc036878d) feat: Add udf grpc support Fixes #145  (#146)
 * [511faffc](https://github.com/numaproj/numaflow/commit/511faffcb7ee6860d15d97dd54332a14300d88f8) refactor: some refactor on watermark (#149)
 * [7fe40c42](https://github.com/numaproj/numaflow/commit/7fe40c428c50af435b54be8d512cae40b6b7e49e) fix: Fixed JS bug (#144)
 * [24a16a04](https://github.com/numaproj/numaflow/commit/24a16a049f2f3a1752ee702f5020136a51d66e69) bug: watermark needs nil check
 * [f4ed831b](https://github.com/numaproj/numaflow/commit/f4ed831ba0d0cbde92f1b7cc1113c83a0c77b702) fix: pipeline UI broken when vertex scaling down to 0 (#132)
 * [0ae0377f](https://github.com/numaproj/numaflow/commit/0ae0377f6019d42dfd0625d86304299061cb18c8) feat: JetStream auto-reconnection (#127)
 * [2fc04eb3](https://github.com/numaproj/numaflow/commit/2fc04eb3b87f42526c21c8648e7aa89a20c933f1) feat: Add watermark for sink vertex (#124)
 * [d958ee6d](https://github.com/numaproj/numaflow/commit/d958ee6defda7e37dcf2192609634bb4d5f97be1) feat: autoscaling with back pressure factor (#123)
 * [b1f77682](https://github.com/numaproj/numaflow/commit/b1f776821e10231292fbc50dce1639fd492a61af) feat: add watermark to UI (#122)
 * [7feeaa87](https://github.com/numaproj/numaflow/commit/7feeaa87996dc173438b0259902266e66d05077b) feat: add processing rate to UI (#121)
 * [43fae931](https://github.com/numaproj/numaflow/commit/43fae931e947c90e3a15c62f65d9cacaf48bbcfa) feat: Expose watermark over HTTP (#120)
 * [ec02304a](https://github.com/numaproj/numaflow/commit/ec02304a15e7436b0559f6443b2ab86d186067fe) fix: daemon service rest api not working (#119)
 * [f3da56d3](https://github.com/numaproj/numaflow/commit/f3da56d36c23c9e356aa689d81348e7e21801d90) chore(deps): bump terser from 5.14.1 to 5.14.2 in /ui (#117)
 * [e2e63c84](https://github.com/numaproj/numaflow/commit/e2e63c84c77eb874629b5f547a2383da2f96e7d8) feat: Numaflow autoscaling (#115)
 * [e5da3f54](https://github.com/numaproj/numaflow/commit/e5da3f544e2f755d608c73d98c0aed108b813197) feat: watermark for headoffset (#116)
 * [a45b2eed](https://github.com/numaproj/numaflow/commit/a45b2eed124248a28460309a3ea472769c7562ef) feat: support namespace scope installation (#112)
 * [ce39199e](https://github.com/numaproj/numaflow/commit/ce39199e76150fb1c88bfad35c92e57c23ea4b3a) feat: Expose ReadTimeoutSeconds on Vertex (#110)
 * [18ad1c5f](https://github.com/numaproj/numaflow/commit/18ad1c5fbe2305632011e67d6e239cc8ab1f8f97) fix: imagepullpocily for local testing (#113)
 * [469849b5](https://github.com/numaproj/numaflow/commit/469849b5b9c29889ee38f0712ad2267088bdda5c) feat: removed udfWorkers from limits and added some docs (#103)
 * [3fada667](https://github.com/numaproj/numaflow/commit/3fada667357ae9ead741ad24e7ba33b7cebcbf99) feat: Add icon and other minor changes (#94)
 * [a81838d7](https://github.com/numaproj/numaflow/commit/a81838d7baa0f0b5001aa38cb2e6627bf9b2d977) feat: end to end tickgen watermark validation (#98)
 * [d7d93175](https://github.com/numaproj/numaflow/commit/d7d93175a93104e28e375d61e2dd33669764ef42) fix: Broken hyperlink (#96)
 * [a2e07926](https://github.com/numaproj/numaflow/commit/a2e079264dfc87b65018c57723119ccbe512c99a) add no-op KV Store (#91)
 * [45c8cb69](https://github.com/numaproj/numaflow/commit/45c8cb69dc9ebe2352f58e7cd71eb798bc542384) feat: no operation watermark progressor (#90)
 * [448c229a](https://github.com/numaproj/numaflow/commit/448c229ab7d399505b616a65a9916cac00db3f4d) feat: kafka source pending messages (#80)
 * [1aa39300](https://github.com/numaproj/numaflow/commit/1aa39300ec8c46e262d9203dbaa4b6c4d72490ce) feat: Interface for Watermark (#82)
 * [be78c523](https://github.com/numaproj/numaflow/commit/be78c5237358fe6325a3a2609ea4597ea51257ab) feat: expose pending messages and processing rate (#79)
 * [df30f2a8](https://github.com/numaproj/numaflow/commit/df30f2a84d13d535b226aaf0feb017ceb1952664) feat: Added the right way to decipher from and to vertex (#78)
 * [639c459a](https://github.com/numaproj/numaflow/commit/639c459ac2ad92adabe618ad162d18dab45f5858) feat: define buffer limits on edges (#70)
 * [41fdd38b](https://github.com/numaproj/numaflow/commit/41fdd38bd102ad91ad75e9d8a260a818762ec91d) feat: Merge UX server code (#67)
 * [ced99079](https://github.com/numaproj/numaflow/commit/ced9907908b3230f7f909341401d8d4934381240) feat: auto-scaling (part 1) (#59)
 * [fd5b3741](https://github.com/numaproj/numaflow/commit/fd5b37412ab0cb66f2399f90b856b960e570e368) Added name to service spec (#58)
 * [dc2badfd](https://github.com/numaproj/numaflow/commit/dc2badfdc046c214aef71f32a9cd3a60038bff41) feat: introduce source buffer and sink buffer (#53)
 * [4ed83a2a](https://github.com/numaproj/numaflow/commit/4ed83a2aede73510da17aab5431c9e3e549a5d47) feat: async publishing for kafka sink (#51)
 * [9f9f5ba7](https://github.com/numaproj/numaflow/commit/9f9f5ba73a4bcfb766085c349dffdde15ce32135) fix spelling errors (#48)
 * [f423002e](https://github.com/numaproj/numaflow/commit/f423002efb8f307c995bbf59e63fb7bc52d85d31) feat: controller to create buckets (#47)
 * [8328739c](https://github.com/numaproj/numaflow/commit/8328739c6534473a3892aeaedc4261b43449efc4) turn on watermark only if ENV value is true (#46)
 * [46f72e23](https://github.com/numaproj/numaflow/commit/46f72e237d5a153be6c59bd736130fe70abaf1e0) minimal end to end line-graph watermark integration (#43)
 * [1f8203f4](https://github.com/numaproj/numaflow/commit/1f8203f4ad13ae4e9713373d7858f0096957c93e) Fixed spelling error (#44)
 * [f1e99eae](https://github.com/numaproj/numaflow/commit/f1e99eae3b9e5a49cc651f8ef47a912329549960) Exponential buckets (#42)
 * [dfcfdeba](https://github.com/numaproj/numaflow/commit/dfcfdeba8b2c86661147ef53707aa2d3f46c5074) fix: different behavior for time.After in go 1.18 (#39)

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

 * [ac15d229](https://github.com/numaproj/numaflow/commit/ac15d229af7ba127c162815d83508bb62b6b35b5) Update manifests to v0.5.6
 * [f2363757](https://github.com/numaproj/numaflow/commit/f2363757fca2b51cc466afe344fb54215c4c5051) feat: grpc udsink (#174)
 * [2650c2de](https://github.com/numaproj/numaflow/commit/2650c2de59f1903a269dd3c15af0e0c285e5d290) fix: numaflow-go udf example & docs (#177)
 * [c44f733f](https://github.com/numaproj/numaflow/commit/c44f733f2b20c1e2d75664f4b71066b35ea6bc3b) fix: use scale.max if it is set (#179)
 * [39e92d06](https://github.com/numaproj/numaflow/commit/39e92d06380cba339f692251d7ca319b5fc481cb) fix broken path (#176)
 * [46ce0f87](https://github.com/numaproj/numaflow/commit/46ce0f879758b38153d7d4a58c00e8714ce8871d) feat: Shuffle implementation (#169)
 * [71ca00a1](https://github.com/numaproj/numaflow/commit/71ca00a1b425706d7ef7bbc6f5f19ff8f2718305) feat: windowing operations (#157)
 * [ca00b78f](https://github.com/numaproj/numaflow/commit/ca00b78f2d8ece0bfee426ea2f2b2216fa24f127) feat: watermark for sources (#159)
 * [91e21cee](https://github.com/numaproj/numaflow/commit/91e21ceec78ced5ad53979c36b70be39198a5af5) pbq implementation (#155)
 * [65424004](https://github.com/numaproj/numaflow/commit/654240042a1d302166fdbc1ac367c92bc052b19a) feat: check if udf is running in liveness probe  (#156)
 * [79dce0b3](https://github.com/numaproj/numaflow/commit/79dce0b386d7fec7a008a1d0a1e4d7bf9835ecaa) feat: Add udf grpc support Fixes #145  (#146)

### Contributors

 * Chrome
 * Derek Wang
 * Juanlu Yu
 * Vigith Maurice
 * Yashash H L

## v0.5.5 (2022-09-07)

 * [9aae638c](https://github.com/numaproj/numaflow/commit/9aae638c4bc0011e027b40c4b7a3b17b189ea945) Update manifests to v0.5.5
 * [32414325](https://github.com/numaproj/numaflow/commit/324143252c3657b85c643b074949679dcd4f26ee) fix: daemon service client memory leak (#161)
 * [be47a26a](https://github.com/numaproj/numaflow/commit/be47a26af5965897951da35a53eff6d5f423df89) refactor: some refactor on watermark (#149)
 * [857cce75](https://github.com/numaproj/numaflow/commit/857cce75b86d7aa96cdeebbf822bfb52607e05da) fix: Fixed JS bug (#144)
 * [da16abc7](https://github.com/numaproj/numaflow/commit/da16abc7e623d0e25564fdb64cc6c3f01c23e88d) bug: watermark needs nil check
 * [c9998a1c](https://github.com/numaproj/numaflow/commit/c9998a1ca3926f37d2180e1082beacdb24d0b3b1) fix: pipeline UI broken when vertex scaling down to 0 (#132)

### Contributors

 * Derek Wang
 * Krithika3
 * Vigith Maurice

## v0.5.4 (2022-08-05)

 * [57513b40](https://github.com/numaproj/numaflow/commit/57513b408eddd8e7918cab540ea866ad19d13518) Update manifests to v0.5.4
 * [94cdb82f](https://github.com/numaproj/numaflow/commit/94cdb82febe92021a37ea44c466949982da13910) feat: JetStream auto-reconnection (#127)
 * [8d835408](https://github.com/numaproj/numaflow/commit/8d8354082966a524275539dfc2c31e2c2a2c47bc) feat: Add watermark for sink vertex (#124)
 * [228ba321](https://github.com/numaproj/numaflow/commit/228ba3216bdfa1b667407be8310e5561f5fea90e) feat: autoscaling with back pressure factor (#123)
 * [9833efdf](https://github.com/numaproj/numaflow/commit/9833efdf14892f20ea792a042d03adec4ad3a91a) feat: add watermark to UI (#122)
 * [0dab55d8](https://github.com/numaproj/numaflow/commit/0dab55d8707a7172b37e4e59053ea0d770520982) feat: add processing rate to UI (#121)
 * [ffd38a15](https://github.com/numaproj/numaflow/commit/ffd38a1528214f2a09986459f3a14588276fbbe0) feat: Expose watermark over HTTP (#120)
 * [c09502a2](https://github.com/numaproj/numaflow/commit/c09502a286b8f35ede0ccc43545afc391d967e58) fix: daemon service rest api not working (#119)
 * [ebc10f41](https://github.com/numaproj/numaflow/commit/ebc10f4117d5ee19dbe6ad4915b7f63f14325373) chore(deps): bump terser from 5.14.1 to 5.14.2 in /ui (#117)
 * [84490ca8](https://github.com/numaproj/numaflow/commit/84490ca852f95f661fbe93687180672ad5ecacca) feat: Numaflow autoscaling (#115)
 * [32b98486](https://github.com/numaproj/numaflow/commit/32b98486f4c7f004ed4b36be5f4af18e29d71969) feat: watermark for headoffset (#116)
 * [283dae90](https://github.com/numaproj/numaflow/commit/283dae9073f59de35b82ba3b4d243204d9d77067) feat: support namespace scope installation (#112)
 * [8e612b1f](https://github.com/numaproj/numaflow/commit/8e612b1fa2b3ca3c0ad037ab816b7ddc1322dd7d) feat: Expose ReadTimeoutSeconds on Vertex (#110)
 * [d95d41bd](https://github.com/numaproj/numaflow/commit/d95d41bd6446cd0b1312b93da4a88dd305b29ce4) fix: imagepullpocily for local testing (#113)

### Contributors

 * Derek Wang
 * Krithika3
 * Saravanan Balasubramanian
 * Sidhant Kohli
 * Vigith Maurice
 * dependabot[bot]

## v0.5.3 (2022-07-08)

 * [efee5442](https://github.com/numaproj/numaflow/commit/efee5442c7618959319a1825f467f059fe67ac57) Update manifests to v0.5.3
 * [5895facd](https://github.com/numaproj/numaflow/commit/5895facd75b3fe2ba296a7283bca61e9b7b9e4e5) feat: removed udfWorkers from limits and added some docs (#103)
 * [0b75495f](https://github.com/numaproj/numaflow/commit/0b75495f1b5795de619cb19430d2b125457e119a) feat: Add icon and other minor changes (#94)
 * [7eb08f58](https://github.com/numaproj/numaflow/commit/7eb08f58b7730357ed5c827c45ceda6177f5cc37) feat: end to end tickgen watermark validation (#98)
 * [3338e658](https://github.com/numaproj/numaflow/commit/3338e6589c7910b30813e9a9912916956f1d3a7e) fix: Broken hyperlink (#96)
 * [e3112229](https://github.com/numaproj/numaflow/commit/e311222937098b49d17f2167f9002adefa1e2461) add no-op KV Store (#91)
 * [5d2f90ed](https://github.com/numaproj/numaflow/commit/5d2f90ed0af7adbd2e2ddffd96b71577ce78e604) feat: no operation watermark progressor (#90)
 * [f58d0f49](https://github.com/numaproj/numaflow/commit/f58d0f4989a1569e45a6b66056e99db46f2b3218) feat: kafka source pending messages (#80)
 * [cbb16ca2](https://github.com/numaproj/numaflow/commit/cbb16ca23db1054c8870b279fad47c183e5ad76a) feat: Interface for Watermark (#82)
 * [5592bb1b](https://github.com/numaproj/numaflow/commit/5592bb1b453bf00be2c756487614700820a6c95f) feat: expose pending messages and processing rate (#79)
 * [06a3df2d](https://github.com/numaproj/numaflow/commit/06a3df2d310f8d52bcb062c7d3e6a249723796d5) feat: Added the right way to decipher from and to vertex (#78)
 * [a0908ad4](https://github.com/numaproj/numaflow/commit/a0908ad49a356b7f1cd40d40e5e8efd7f1994205) feat: define buffer limits on edges (#70)
 * [a1d36395](https://github.com/numaproj/numaflow/commit/a1d363955b4c4a2ff9bddb821e43a955e370fc4c) feat: Merge UX server code (#67)
 * [571c48eb](https://github.com/numaproj/numaflow/commit/571c48eb039bc8b4e27e87b8f959aa2d72f56f23) feat: auto-scaling (part 1) (#59)
 * [1e0384ba](https://github.com/numaproj/numaflow/commit/1e0384ba76994b68cb75a0967cb1e0460bc19b75) Added name to service spec (#58)

### Contributors

 * Derek Wang
 * Krithika3
 * Sidhant Kohli
 * Vigith Maurice

## v0.5.2 (2022-06-13)

 * [2f2d10ce](https://github.com/numaproj/numaflow/commit/2f2d10cebf7158c83e1febb0b06e8e8e002a32cd) Update manifests to v0.5.2
 * [cedd0d1f](https://github.com/numaproj/numaflow/commit/cedd0d1f8ef752fea1464799d90bff2fe009fe0d) feat: introduce source buffer and sink buffer (#53)
 * [d3301aa9](https://github.com/numaproj/numaflow/commit/d3301aa9f0c3ae3771096422ec114686e7f7c21c) feat: async publishing for kafka sink (#51)
 * [2474eb8e](https://github.com/numaproj/numaflow/commit/2474eb8ec3deaf132ee30e7881dddd3ac7460e18) fix spelling errors (#48)
 * [c4a12f87](https://github.com/numaproj/numaflow/commit/c4a12f87c7aedb6f6ddda07e2d776a3a2c1b5c6a) feat: controller to create buckets (#47)
 * [eb97dc3b](https://github.com/numaproj/numaflow/commit/eb97dc3bdc8e41d934c4ca17e7a97dcd192d3870) turn on watermark only if ENV value is true (#46)
 * [f189ba30](https://github.com/numaproj/numaflow/commit/f189ba30e4e7f8beb5d4340c377e84574a2092cd) minimal end to end line-graph watermark integration (#43)

### Contributors

 * Derek Wang
 * Qianbo Huai
 * Vigith Maurice

## v0.5.1 (2022-06-02)

 * [bb9be807](https://github.com/numaproj/numaflow/commit/bb9be807eddb68bf70a8e64e285d631ff3a1c4e0) Update manifests to v0.5.1
 * [912747eb](https://github.com/numaproj/numaflow/commit/912747eb0cabac2a78950155bf5e37e7fe3a5e8b) Fixed spelling error (#44)
 * [3aeb33a8](https://github.com/numaproj/numaflow/commit/3aeb33a8709591b9cb0a14d55e3c44fd5f031437) Exponential buckets (#42)
 * [1d656829](https://github.com/numaproj/numaflow/commit/1d656829e19f31111356d0e7a74d83c887a87dd0) fix: different behavior for time.After in go 1.18 (#39)

### Contributors

 * Derek Wang
 * Krithika3

