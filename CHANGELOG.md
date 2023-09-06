# Changelog

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

