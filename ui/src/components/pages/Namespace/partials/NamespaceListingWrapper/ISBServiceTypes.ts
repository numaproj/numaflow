export interface ISBServiceListingData {
  data: ISBServicesListing[];
}

export interface ISBServicesListing {
  name: string;
  status: string;
  isbService: IsbServiceListing;
}

export interface IsbServiceListing {
  kind: string;
  apiVersion: string;
  metadata: Metadata;
  spec: Spec;
  status: Status;
}

interface Status {
  conditions: Condition[];
  phase: string;
  config: Config;
  type: string;
}

interface Config {
  jetstream: Jetstream2;
}

interface Jetstream2 {
  url: string;
  auth: Auth;
  streamConfig: string;
}

interface Auth {
  basic: Basic;
}

interface Basic {
  user: User;
  password: User;
}

interface User {
  name: string;
  key: string;
}

interface Condition {
  type: string;
  status: string;
  lastTransitionTime: string;
  reason: string;
  message: string;
}

interface Spec {
  jetstream: Jetstream;
}

interface Jetstream {
  version: string;
  replicas: number;
  persistence: Persistence;
}

interface Persistence {
  volumeSize: string;
}

interface Metadata {
  name: string;
  namespace: string;
  uid: string;
  resourceVersion: string;
  generation: number;
  creationTimestamp: string;
  annotations?: Annotations;
  finalizers: string[];
  managedFields: ManagedField[];
}

interface ManagedField {
  manager: string;
  operation: string;
  apiVersion: string;
  time: string;
  fieldsType: string;
  fieldsV1: FieldsV1;
  subresource?: string;
}

interface FieldsV1 {
  "f:metadata"?: Fmetadata;
  "f:spec"?: Fspec;
  "f:status"?: Fstatus;
}

interface Fstatus {
  ".": _;
  "f:conditions": _;
  "f:config": Fconfig;
  "f:phase": _;
  "f:type": _;
}

interface Fconfig {
  ".": _;
  "f:jetstream": Fjetstream2;
}

interface Fjetstream2 {
  ".": _;
  "f:auth": Fauth;
  "f:streamConfig": _;
  "f:url": _;
}

interface Fauth {
  ".": _;
  "f:basic": Fbasic;
}

interface Fbasic {
  ".": _;
  "f:password": Fpassword;
  "f:user": Fpassword;
}

interface Fpassword {
  ".": _;
  "f:key": _;
  "f:name": _;
}

interface Fspec {
  ".": _;
  "f:jetstream": Fjetstream;
}

interface Fjetstream {
  ".": _;
  "f:persistence": Fpersistence;
  "f:replicas": _;
  "f:version": _;
}

interface Fpersistence {
  ".": _;
  "f:volumeSize": _;
}

interface Fmetadata {
  "f:annotations"?: Fannotations;
  "f:finalizers"?: Ffinalizers;
}

interface Ffinalizers {
  ".": _;
  'v:"isbsvc-controller"': _;
}

interface Fannotations {
  ".": _;
  "f:kubectl.kubernetes.io/last-applied-configuration": _;
}

interface _ {}

interface Annotations {
  "kubectl.kubernetes.io/last-applied-configuration": string;
}
