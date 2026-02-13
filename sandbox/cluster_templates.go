// DBDeployer - The MySQL Sandbox
// Copyright © 2006-2019 Giuseppe Maxia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build go1.16
// +build go1.16

package sandbox

import (
	_ "embed"

	"github.com/datacharmer/dbdeployer/globals"
)

// Templates for InnoDB Cluster

var (
	//go:embed templates/cluster/innodb_cluster_options.gotxt
	clusterOptionsTemplate string

	//go:embed templates/cluster/innodb_cluster_options84.gotxt
	clusterOptions84Template string

	//go:embed templates/cluster/init_cluster_nodes.gotxt
	initClusterNodesTemplate string

	//go:embed templates/cluster/check_nodes_cluster.gotxt
	checkClusterNodesTemplate string

	ClusterTemplates = TemplateCollection{
		globals.TmplClusterOptions: TemplateDesc{
			Description: "Set the correct my.cnf configurations",
			Notes:       "",
			Contents:    clusterOptionsTemplate,
		},
		globals.TmplClusterOptions84: TemplateDesc{
			Description: "Set the correct my.cnf configurations for 8.4.x",
			Notes:       "",
			Contents:    clusterOptions84Template,
		},
		globals.TmplInitializeNodesCluster: TemplateDesc{
			Description: "Initialize InnoDB Cluster nodes using MySQL Shell",
			Notes:       "",
			Contents:    initClusterNodesTemplate,
		},
		globals.TmplCheckClusterNodes: TemplateDesc{
			Description: "Checks the status of group replication",
			Notes:       "",
			Contents:    checkClusterNodesTemplate,
		},
	}
)
