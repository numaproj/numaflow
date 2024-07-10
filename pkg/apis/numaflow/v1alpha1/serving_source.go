package v1alpha1

type Serving struct {
	// +optional
	Auth *Authorization `json:"auth" protobuf:"bytes,1,opt,name=auth"`
	// Whether to create a ClusterIP Service
	// +optional
	Service bool `json:"service" protobuf:"bytes,2,opt,name=service"`
	// The header key from which the message id will be extracted
	// Default
	// +optional
	MsgIDHeaderKey string `json:"msgIDHeaderKey" protobuf:"bytes,3,opt,name=msgIDHeaderKey"`
	// Persistent store for the callbacks
	// +optional
	CallbackStorage *CallbackStorage `json:"callbackStorage" protobuf:"bytes,4,opt,name=callbackStorage"`
}

type CallbackStorage struct {
	// URL of the persistent store to write the callbacks
	URL string `json:"url" protobuf:"bytes,1,opt,name=url"`
	// TODO auth
}

// we will send the vertex object as json
// jetstream details as environment variables (stream name, js_url, js_user, js_password)
// db details as environment variables (db_url, db_user, db_password)

// start the container with rust code and pass all these environment variables.
/*
Goal:
	The goal is to have a control plane to manage the Numaserve deployment. In phase 1 we decided not support Numaserve to deployed independently
	without Numaflow so we will have to enhance the Numaflow controller to manage the Numaserve deployment. The controller will be responsible for
	creating the serving source container and the necessary resources for the serving source to work.
*/

/*
Design details:
	Numaserve is going to another builtin source in the Numaflow, since it exposes the endpoints to the users to write the payload. As a serving source
 	it will expose the endpoints for the users to write the payload in both sync and async fashion also it will expose the endpoint for the user to track
	the message given the message id.

    Some of the options we have considered:

	Numaserve as a complete independent builtin source:
		* Numaserve will be a builtin source which will be managed by the Numaflow controller. The problem with this approach is that Numaserve will have
		  to do all the functionality that the normal source does like publishing metrics, support for source transformer and publishing watermarks etc.
          The current Numaserve is being written in Rust and it will be a lot of work to replicate the functionality that the normal source does and forwards
          the message to ISB.

	Numaserve as a sidecar container with builtin jetstream source:
		* Numaserve will be deployed as a sidecar inside jetstream source and numaserve will expose endpoints and it will write the payload with the
          required headers to the jetstream source's stream and jetstream source will be reading from the stream and forward the message to the next ISB.
	      Since its the main container it already has the functionality to publish watermarks, support source transformer and other Numaflow core functionalities.


	We decided to go with second approach since it is easier and Numaserve don't have to do all the things the Numaflow does.


*/
// When serving source is configured
/*
 Flow:
	- User writes the payload to the serving endpoint.
	- Serving container will get the payload and attaches necessary headers to the payload and writes it to the jetstream stream.
	- Jetstream source will consume the payload and forwards it to the next vertex (normal pipeline flow).
    - Each vertex after processing the payload will write a callback to the serving container by using the pod id present in the header.
	- Serving container will be listening to the callbacks and will write the response back to the user once we get all the callbacks for a request.

Things controller needs to do:
 * Create a separate container for serving source which will have the serving image running.
 * Create a jetstream stream for the serving source to write the payload and for the jetstream source(numa container) to get the payload.
 * Configure numa container to use jetstream source which reads the payload from the stream and forwards it to the next vertex.
 * Create a ClusterIP service for the serving source if asked by the user.
 * Mount the config map required by the serving source container which has different configurations like stream name, redis connection details etc.
 * Mount the pipeline json config map to the serving source container.
 * Configure liveliness and readiness probes for the serving source container.


Things to do on serving source:
 * Publish a separate image for the serving source to quay.io.
 * Update the serving source to write the payload to the jetstream isb.
 * Support for auth for jetstream connection, get the username and password from the environment variables.
 * Update the serving source get the pipeline json from the environment.?
 * Do we really need config map? Since we pass everything as environment variables.?
 * Set up liveliness and readiness probes for the serving source container.
*/

/*
 Open items:
  - How to track async requests? Do we really need to expose an async endpoint?
  - How to use the message_path endpoint? Do we need to expose it?
  - What should be the name of the serving source image?
*/
