# Nutanix Prism API to SQL Server
Fetches data from the Nutanix Prism API and inserts it into SQL Server

## Clusters and Cluster Hosts
This will sync all registered clusters to a table in SQL Server with the following format:

| id | name | deleted |
|----|------|---------|
| aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee | cluster-name | NULL |
| aaaaaaaa-bbbb-cccc-dddd-ffffffffffff | cluster-name-2 | 2021-01-01T00:00:01Z |

```yaml
  - name: Sync Clusters
    source: nutanix
    destination: sqlserver
    sourceConfig:
        listRequest:
        path: v3/hosts/list
        method: POST
        body_type: application/json
        body:
            kind: host
        response:
            length: metadata.total_matches
            items: entities
    destinationConfig:
        table: ntnx_cluster
        query: SELECT * FROM [NUTANIX_DB].[dbo].[ntnx_cluster] WHERE deleted IS NULL
        soft_delete: true
        deleted_field: deleted
        deleted_value: GETDATE()
    sourceTransformers:
        - transformer: strings.set_default
        config:
            inField: status.name
            defaultValue: no-name-provided
        - transformer: strings.uppercase
        config:
            inOutMap:
            status.name: name
            metadata.uuid: id
    comparisonColumns:
      - name: id
          type: Utf8
          unique: true
      - name: name
          type: Utf8

  - name: Sync Cluster Hosts
    source: nutanix
    destination: sqlserver
    sourceConfig:
      listRequest:
        path: v3/hosts/list
        method: POST
        body_type: application/json
        body:
          kind: host
        response:
          length: metadata.total_matches
          items: entities
    destinationConfig:
      table: ntnx_cluster_host
      query: SELECT * FROM [NUTANIX_DB].[dbo].[ntnx_cluster_host] WHERE deleted IS
        NULL
      soft_delete: true
      deleted_field: deleted
      deleted_value: GETDATE()
    sourceTransformers:
      - transformer: strings.set_default
        config:
          inField: status.name
          defaultValue: no-name-provided
      - transformer: strings.uppercase
        config:
          inOutMap:
            status.name: name
            metadata.uuid: id
            status.cluster_reference.uuid: cluster_id
      - transformer: frames.rename_columns
        config:
          columns:
            - from: status.resources.ipmi.ip
              to: ip_address
            - from: status.resources.controller_vm.ip
              to: controller_vm_ip
    comparisonColumns:
      - name: id
        type: Utf8
        unique: true
      - name: ip_address
        type: Utf8
      - name: controller_vm_ip
        type: Utf8
      - name: cluster_id
        type: Utf8
      - name: name
        type: Utf8
```

## Subnets/Networks
Sync subnets and networks from Nutanix Prism API to SQL Server in the following format: 

| id | cluster_id | name | vlan | vswitch_id | vswitch_name | deleted |
|----|------------|------|------|------------|--------------|---------|
| aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee | aaaaaaaa-bbbb-cccc-dddd-ffffffffffff | subnet-name | 100 | aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee | vswitch-name | NULL |
| aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeef | aaaaaaaa-bbbb-cccc-dddd-ffffffffffff | subnet-name-2 | 200 | aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee | vswitch-name | 2021-01-01T00:00:01Z |

```yaml
  - name: Sync Subnets/Networks
    source: nutanix
    destination: sqlserver
    sourceConfig:
      listRequest:
        path: v3/subnets/list
        method: POST
        body_type: application/json
        body:
          kind: subnet
          length: 999
        response:
          length: metadata.total_matches
          items: entities
      columns:
        - name: metadata.uuid
          type: Utf8
          unique: true
        - name: status.cluster_reference.uuid
          type: Utf8
          unique: false
        - name: status.name
          type: Utf8
          unique: false
        - name: status.resources.vlan_id
          type: Int32
          unique: false
        - name: status.resources.virtual_switch_uuid
          type: Utf8
          unique: false
        - name: status.resources.vswitch_name
          type: Utf8
          unique: false
    destinationConfig:
      table: ntnx_subnet
      query: SELECT * FROM [NUTANIX_DB].[dbo].[ntnx_subnet] WHERE deleted IS NULL
      soft_delete: true
      deleted_field: deleted
      deleted_value: GETDATE()
      columns:
        - name: id
          type: Utf8
          unique: true
        - name: cluster_id
          type: Utf8
          unique: false
        - name: name
          type: Utf8
          unique: false
        - name: vlan
          type: Int32
          unique: false
        - name: vswitch_id
          type: Utf8
          unique: false
        - name: vswitch_name
          type: Utf8
          unique: false
    sourceTransformers:
      - transformer: strings.uppercase
        config:
          inOutMap:
            metadata.uuid: id
            status.name: name
            status.cluster_reference.uuid: cluster_id
            status.resources.virtual_switch_uuid: vswitch_id
      - transformer: frames.rename_columns
        config:
          columns:
            - from: status.resources.vlan_id
              to: vlan
            - from: status.resources.vswitch_name
              to: vswitch_name
```

## Virtual Machines, Disks and NICs
This section is going to synchronize Virtual Machines, Disks and NICs to three separate tables, using some of the more advanced transformers

Virtual Machines:

| id | cluster_id | name | ip_address | threads_per_core | vcpu_per_socket | num_sockets | memory_mb | description | departments | deleted |
|----|------------|------|------------|------------------|------------------|------------|-----------|-------------|-------------|---------|
| aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee | aaaaaaaa-bbbb-cccc-dddd-ffffffffffff | vm-name | 1.1.1.1 | 2 | 2 | 2 | 4096 | vm-description | department-1,department-2 | NULL |
| aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeef | aaaaaaaa-bbbb-cccc-dddd-ffffffffffff | vm-name-2 | 2.2.2.2 | 4 | 4 | 4 | 8192 | vm-description-2 | department-3,department-4 | 2021-01-01T00:00:01Z |

Virtual Machine NICs:

| id | vm_id | mac_address | subnet_id | ip_address | deleted |
|----|-------|-------------|-----------|------------|---------|
| aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee | aaaaaaaa-bbbb-cccc-dddd-ffffffffffff | 00:00:00:00:00:00 | aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee | NULL |
| aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeef | aaaaaaaa-bbbb-cccc-dddd-ffffffffffff | 00:00:00:00:00:01 | aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee | 2021-01-01T00:00:01Z |

Virtual Machine Disks: 

| id | vm_id | device_type | size_mb | bus_id | deleted |
|----|-------|-------------|---------|--------|---------|
| aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee | aaaaaaaa-bbbb-cccc-dddd-ffffffffffff | disk | 1024 | 0 | NULL |
| aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeef | aaaaaaaa-bbbb-cccc-dddd-ffffffffffff | disk | 2048 | 1 | 2021-01-01T00:00:01Z |



```yaml
  - name: Sync Virtual Machines
    source: nutanix
    destination: sqlserver
    sourceConfig:
      listRequest:
        path: v3/vms/list
        method: POST
        body_type: application/json
        body:
          kind: vm
          length: 999
        response:
          length: metadata.total_matches
          items: entities
      columns:
        - name: metadata.uuid
          type: Utf8
          unique: true
        - name: status.cluster_reference.uuid
          type: Utf8
          unique: false
        - name: status.name
          type: Utf8
          unique: false
        - name: status.resources.nic_list
          type: List
          unique: false
        - name: status.resources.num_threads_per_core
          type: Int32
          unique: false
        - name: status.resources.num_vcpus_per_socket
          type: Int32
          unique: false
        - name: status.resources.num_sockets
          type: Int32
          unique: false
        - name: status.resources.memory_size_mib
          type: Int32
          unique: false
        - name: status.description
          type: Utf8
          unique: false
        - name: metadata.categories_mapping.Grupperingar
          type: List
    destinationConfig:
      table: ntnx_vm
      query: SELECT * FROM [NUTANIX_DB].[dbo].[ntnx_vm] WHERE deleted IS NULL
      soft_delete: true
      deleted_field: deleted
      deleted_value: GETDATE()
      columns:
        - name: id
          type: Utf8
          unique: true
        - name: cluster_id
          type: Utf8
          unique: false
        - name: name
          type: Utf8
          unique: false
        - name: ip_address
          type: Utf8
          unique: false
        - name: threads_per_core
          type: Int32
          unique: false
        - name: vcpu_per_socket
          type: Int32
          unique: false
        - name: num_sockets
          type: Int32
          unique: false
        - name: memory_mb
          type: Int32
          unique: false
        - name: description
          type: Utf8
          unique: false
        - name: departments
          type: Utf8
          unique: false
    sourceTransformers:
      - transformer: structs.jsonpath
        config:
          inField: status.resources.nic_list
          outField: ip_address
          jsonPath: 0.ip_endpoint_list.0.ip
          defaultValue: ""
      - transformer: strings.uppercase
        config:
          inOutMap:
            metadata.uuid: id
            status.name: name
            status.cluster_reference.uuid: cluster_id
      - transformer: frames.rename_columns
        config:
          columns:
            - from: status.resources.num_threads_per_core
              to: threads_per_core
            - from: status.resources.num_vcpus_per_socket
              to: vcpu_per_socket
            - from: status.resources.num_sockets
              to: num_sockets
            - from: status.resources.memory_size_mib
              to: memory_mb
            - from: status.description
              to: description
      - transformer: strings.join_listfield
        config:
          inField: metadata.categories_mapping.Grupperingar
          outField: departments
          separator: ","

  - name: Sync Virtual Machine NICs
    source: nutanix
    destination: sqlserver
    sourceConfig:
      listRequest:
        path: v3/vms/list
        method: POST
        body_type: application/json
        body:
          kind: vm
          length: 999
        response:
          length: metadata.total_matches
          items: entities
      columns:
        - name: id
          type: Utf8
          unique: true
        - name: vm_id
          type: Utf8
          unique: false
        - name: mac_address
          type: Utf8
          unique: false
        - name: subnet_id
          type: Utf8
          unique: false
        - name: ip_address
          type: Utf8
          unique: false
    destinationConfig:
      table: ntnx_vm_dev_nic
      query: SELECT * FROM [NUTANIX_DB].[dbo].[ntnx_vm_dev_nic] WHERE deleted IS
        NULL
      soft_delete: true
      deleted_field: deleted
      deleted_value: GETDATE()
      columns:
        - name: id
          type: Utf8
          unique: true
        - name: vm_id
          type: Utf8
          unique: false
        - name: mac_address
          type: Utf8
          unique: false
        - name: subnet_id
          type: Utf8
          unique: false
        - name: ip_address
          type: Utf8
          unique: false
    sourceTransformers:
      - transformer: frames.extract_nested_rows
        config:
          iterField: status.resources.nic_list
          fieldMap:
            id: uuid
            mac_address: mac_address
            subnet_id: subnet_reference.uuid
            ip_address: ip_endpoint_list.0.ip
          colMap:
            vm_id: metadata.uuid
      - transformer: strings.uppercase
        config:
          inOutMap:
            id: id
            vm_id: vm_id
            mac_address: mac_address
            subnet_id: subnet_id

  - name: Sync Virtual Machine Disks
    source: nutanix
    destination: sqlserver
    sourceConfig:
      listRequest:
        path: v3/vms/list
        method: POST
        body_type: application/json
        body:
          kind: vm
          length: 999
        response:
          length: metadata.total_matches
          items: entities
      columns:
        - name: id
          type: Utf8
          unique: true
        - name: vm_id
          type: Utf8
          unique: false
        - name: device_type
          type: Utf8
          unique: false
        - name: bus_id
          type: Int64
          unique: false
        - name: size_mb
          type: Int64
          unique: false
    destinationConfig:
      table: ntnx_vm_dev_storage
      query: SELECT * FROM [NUTANIX_DB].[dbo].[ntnx_vm_dev_storage] WHERE deleted IS NULL
      soft_delete: true
      deleted_field: deleted
      deleted_value: GETDATE()
      columns:
        - name: id
          type: Utf8
          unique: true
        - name: vm_id
          type: Utf8
          unique: false
        - name: device_type
          type: Utf8
          unique: false
        - name: size_mb
          type: Int32
          unique: false
        - name: bus_id
          type: Int64
          unique: false
    sourceTransformers:
      - transformer: frames.extract_nested_rows
        config:
          iterField: status.resources.disk_list
          fieldMap:
            id: uuid
            device_type: device_properties.device_type
            size_bytes: disk_size_bytes
            bus_id: device_properties.disk_address.device_index
          colMap:
            vm_id: metadata.uuid
      - transformer: strings.uppercase
        config:
          inOutMap:
            id: id
            vm_id: vm_id
      - transformer: int.divide
        config:
          inField: size_bytes
          outField: size_mb
          factor: 1048576
```

## Full Configuration
```yaml
configVersion: V1
sources:
  - name: sqlserver
    type: Sqlserver
    connection:
      settings:
        connection_string: "Host=sqlserver;User=sa;Password=Password123;Database=NUTANIX_DB"
  - name: nutanix
    type: Rest
    connection:
      settings:
        base_url: "https://nutanix.local:9440"
        authentication:
          basic: true
          basic_user: username
          basic_pass: password
        ignore_certificates: true
sync:
  - name: Sync Clusters
    source: nutanix
    destination: sqlserver
    sourceConfig:
      listRequest:
        path: v3/hosts/list
        method: POST
        body_type: application/json
        body:
          kind: host
        response:
          length: metadata.total_matches
          items: entities
      columns:
        - name: metadata.uuid
          type: Utf8
          unique: true
        - name: status.name
          type: Utf8
    destinationConfig:
      table: ntnx_cluster
      query: SELECT * FROM [NUTANIX_DB].[dbo].[ntnx_cluster] WHERE deleted IS NULL
      soft_delete: true
      deleted_field: deleted
      deleted_value: GETDATE()
      columns:
        - name: id
          type: Utf8
          unique: true
        - name: name
          type: Utf8
    sourceTransformers:
      - transformer: strings.set_default
        config:
          inField: status.name
          defaultValue: no-name-provided
      - transformer: strings.uppercase
        config:
          inOutMap:
            status.name: name
            metadata.uuid: id

  - name: Sync Cluster Hosts
    source: nutanix
    destination: sqlserver
    sourceConfig:
      listRequest:
        path: v3/hosts/list
        method: POST
        body_type: application/json
        body:
          kind: host
        response:
          length: metadata.total_matches
          items: entities
      columns:
        - name: metadata.uuid
          type: Utf8
          unique: true
        - name: status.name
          type: Utf8
        - name: status.cluster_reference.uuid
          type: Utf8
        - name: status.resources.ipmi.ip
          type: Utf8
        - name: status.resources.controller_vm.ip
          type: Utf8
    destinationConfig:
      table: ntnx_cluster_host
      query: SELECT * FROM [NUTANIX_DB].[dbo].[ntnx_cluster_host] WHERE deleted IS
        NULL
      soft_delete: true
      deleted_field: deleted
      deleted_value: GETDATE()
      columns:
        - name: id
          type: Utf8
          unique: true
        - name: cluster_id
          type: Utf8
        - name: name
          type: Utf8
        - name: ip_address
          type: Utf8
        - name: controller_vm_ip
          type: Utf8
    sourceTransformers:
      - transformer: strings.set_default
        config:
          inField: status.name
          defaultValue: no-name-provided
      - transformer: strings.uppercase
        config:
          inOutMap:
            status.name: name
            metadata.uuid: id
            status.cluster_reference.uuid: cluster_id
      - transformer: frames.rename_columns
        config:
          columns:
            - from: status.resources.ipmi.ip
              to: ip_address
            - from: status.resources.controller_vm.ip
              to: controller_vm_ip
                            
  - name: Sync Subnets/Networks
    source: nutanix
    destination: sqlserver
    sourceConfig:
      listRequest:
        path: v3/subnets/list
        method: POST
        body_type: application/json
        body:
          kind: subnet
          length: 999
        response:
          length: metadata.total_matches
          items: entities
      columns:
        - name: metadata.uuid
          type: Utf8
          unique: true
        - name: status.cluster_reference.uuid
          type: Utf8
          unique: false
        - name: status.name
          type: Utf8
          unique: false
        - name: status.resources.vlan_id
          type: Int32
          unique: false
        - name: status.resources.virtual_switch_uuid
          type: Utf8
          unique: false
        - name: status.resources.vswitch_name
          type: Utf8
          unique: false
    destinationConfig:
      table: ntnx_subnet
      query: SELECT * FROM [NUTANIX_DB].[dbo].[ntnx_subnet] WHERE deleted IS NULL
      soft_delete: true
      deleted_field: deleted
      deleted_value: GETDATE()
      columns:
        - name: id
          type: Utf8
          unique: true
        - name: cluster_id
          type: Utf8
          unique: false
        - name: name
          type: Utf8
          unique: false
        - name: vlan
          type: Int32
          unique: false
        - name: vswitch_id
          type: Utf8
          unique: false
        - name: vswitch_name
          type: Utf8
          unique: false
    sourceTransformers:
      - transformer: strings.uppercase
        config:
          inOutMap:
            metadata.uuid: id
            status.name: name
            status.cluster_reference.uuid: cluster_id
            status.resources.virtual_switch_uuid: vswitch_id
      - transformer: frames.rename_columns
        config:
          columns:
            - from: status.resources.vlan_id
              to: vlan
            - from: status.resources.vswitch_name
              to: vswitch_name
              
  
  - name: Sync Virtual Machines
    source: nutanix
    destination: sqlserver
    sourceConfig:
      listRequest:
        path: v3/vms/list
        method: POST
        body_type: application/json
        body:
          kind: vm
          length: 999
        response:
          length: metadata.total_matches
          items: entities
      columns:
        - name: metadata.uuid
          type: Utf8
          unique: true
        - name: status.cluster_reference.uuid
          type: Utf8
          unique: false
        - name: status.name
          type: Utf8
          unique: false
        - name: status.resources.nic_list
          type: List
          unique: false
        - name: status.resources.num_threads_per_core
          type: Int32
          unique: false
        - name: status.resources.num_vcpus_per_socket
          type: Int32
          unique: false
        - name: status.resources.num_sockets
          type: Int32
          unique: false
        - name: status.resources.memory_size_mib
          type: Int32
          unique: false
        - name: status.description
          type: Utf8
          unique: false
        - name: metadata.categories_mapping.Grupperingar
          type: List
    destinationConfig:
      table: ntnx_vm
      query: SELECT * FROM [NUTANIX_DB].[dbo].[ntnx_vm] WHERE deleted IS NULL
      soft_delete: true
      deleted_field: deleted
      deleted_value: GETDATE()
      columns:
        - name: id
          type: Utf8
          unique: true
        - name: cluster_id
          type: Utf8
          unique: false
        - name: name
          type: Utf8
          unique: false
        - name: ip_address
          type: Utf8
          unique: false
        - name: threads_per_core
          type: Int32
          unique: false
        - name: vcpu_per_socket
          type: Int32
          unique: false
        - name: num_sockets
          type: Int32
          unique: false
        - name: memory_mb
          type: Int32
          unique: false
        - name: description
          type: Utf8
          unique: false
        - name: departments
          type: Utf8
          unique: false
    sourceTransformers:
      - transformer: structs.jsonpath
        config:
          inField: status.resources.nic_list
          outField: ip_address
          jsonPath: 0.ip_endpoint_list.0.ip
          defaultValue: ""
      - transformer: strings.uppercase
        config:
          inOutMap:
            metadata.uuid: id
            status.name: name
            status.cluster_reference.uuid: cluster_id
      - transformer: frames.rename_columns
        config:
          columns:
            - from: status.resources.num_threads_per_core
              to: threads_per_core
            - from: status.resources.num_vcpus_per_socket
              to: vcpu_per_socket
            - from: status.resources.num_sockets
              to: num_sockets
            - from: status.resources.memory_size_mib
              to: memory_mb
            - from: status.description
              to: description
      - transformer: strings.join_listfield
        config:
          inField: metadata.categories_mapping.Grupperingar
          outField: departments
          separator: ","

  - name: Sync Virtual Machine NICs
    source: nutanix
    destination: sqlserver
    sourceConfig:
      listRequest:
        path: v3/vms/list
        method: POST
        body_type: application/json
        body:
          kind: vm
          length: 999
        response:
          length: metadata.total_matches
          items: entities
      columns:
        - name: id
          type: Utf8
          unique: true
        - name: vm_id
          type: Utf8
          unique: false
        - name: mac_address
          type: Utf8
          unique: false
        - name: subnet_id
          type: Utf8
          unique: false
        - name: ip_address
          type: Utf8
          unique: false
    destinationConfig:
      table: ntnx_vm_dev_nic
      query: SELECT * FROM [NUTANIX_DB].[dbo].[ntnx_vm_dev_nic] WHERE deleted IS
        NULL
      soft_delete: true
      deleted_field: deleted
      deleted_value: GETDATE()
      columns:
        - name: id
          type: Utf8
          unique: true
        - name: vm_id
          type: Utf8
          unique: false
        - name: mac_address
          type: Utf8
          unique: false
        - name: subnet_id
          type: Utf8
          unique: false
        - name: ip_address
          type: Utf8
          unique: false
    sourceTransformers:
      - transformer: frames.extract_nested_rows
        config:
          iterField: status.resources.nic_list
          fieldMap:
            id: uuid
            mac_address: mac_address
            subnet_id: subnet_reference.uuid
            ip_address: ip_endpoint_list.0.ip
          colMap:
            vm_id: metadata.uuid
      - transformer: strings.uppercase
        config:
          inOutMap:
            id: id
            vm_id: vm_id
            mac_address: mac_address
            subnet_id: subnet_id

  - name: Sync Virtual Machine Disks
    source: nutanix
    destination: sqlserver
    sourceConfig:
      listRequest:
        path: v3/vms/list
        method: POST
        body_type: application/json
        body:
          kind: vm
          length: 999
        response:
          length: metadata.total_matches
          items: entities
      columns:
        - name: id
          type: Utf8
          unique: true
        - name: vm_id
          type: Utf8
          unique: false
        - name: device_type
          type: Utf8
          unique: false
        - name: bus_id
          type: Int64
          unique: false
        - name: size_mb
          type: Int64
          unique: false
    destinationConfig:
      table: ntnx_vm_dev_storage
      query: SELECT * FROM [NUTANIX_DB].[dbo].[ntnx_vm_dev_storage] WHERE deleted IS NULL
      soft_delete: true
      deleted_field: deleted
      deleted_value: GETDATE()
      columns:
        - name: id
          type: Utf8
          unique: true
        - name: vm_id
          type: Utf8
          unique: false
        - name: device_type
          type: Utf8
          unique: false
        - name: size_mb
          type: Int32
          unique: false
        - name: bus_id
          type: Int64
          unique: false
    sourceTransformers:
      - transformer: frames.extract_nested_rows
        config:
          iterField: status.resources.disk_list
          fieldMap:
            id: uuid
            device_type: device_properties.device_type
            size_bytes: disk_size_bytes
            bus_id: device_properties.disk_address.device_index
          colMap:
            vm_id: metadata.uuid
      - transformer: strings.uppercase
        config:
          inOutMap:
            id: id
            vm_id: vm_id
      - transformer: int.divide
        config:
          inField: size_bytes
          outField: size_mb
          factor: 1048576
```