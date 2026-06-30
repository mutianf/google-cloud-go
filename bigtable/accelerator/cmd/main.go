// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Binary accelerator runs the in-process gRPC proxy daemon: it listens on a
// Unix domain socket, accepts standard google.bigtable.v2.Bigtable RPCs, and
// forwards each one through an AcceleratorChannel that rides the Jetstream
// session transport.
//
// One daemon serves one (project, instance, appProfile) tuple. Cross-language
// clients (e.g. the Python SDK) spawn the binary, wait for the UDS to become
// connectable, then talk standard Bigtable V2 protos over it.
package main

import (
	"context"
	"flag"
	"log"

	"cloud.google.com/go/bigtable/accelerator"
)

func main() {
	udsPath := flag.String("uds-path", "", "path to unix domain socket (required)")
	project := flag.String("project", "", "GCP project ID (required)")
	instance := flag.String("instance", "", "Bigtable instance ID (required)")
	appProfile := flag.String("app-profile", "", "Bigtable app profile (optional)")
	flag.Parse()

	if *udsPath == "" {
		log.Fatal("Missing required flag: --uds-path")
	}
	if *project == "" {
		log.Fatal("Missing required flag: --project")
	}
	if *instance == "" {
		log.Fatal("Missing required flag: --instance")
	}

	ctx := context.Background()
	channel, err := accelerator.NewAcceleratorChannel(ctx, *project, *instance, *appProfile)
	if err != nil {
		log.Fatalf("failed to construct accelerator channel: %v", err)
	}
	srv := accelerator.NewAcceleratorServer(*udsPath, channel)
	log.Printf("Starting accelerator daemon on UDS=%s project=%s instance=%s app-profile=%q",
		*udsPath, *project, *instance, *appProfile)

	if err := srv.Start(); err != nil {
		log.Fatalf("failed to start accelerator server: %v", err)
	}
	defer srv.Stop()

	// Block until the watchdog (stdin EOF or parent-PID change) trips.
	<-srv.ShutdownChan()
	log.Printf("Teardown signal detected, exiting...")
}
