def insert_14_organizations_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {
                    "static": [
                        {
                            "l1": "Testing_Beetl",
                            "l2": "Beetl City",
                            "l3": "School of Beetl",
                            "l4": "1A",
                        },
                        {
                            "l1": "Testing_Beetl",
                            "l2": "Beetl City",
                            "l3": "School of Beetl",
                            "l4": "1B",
                        },
                        {
                            "l1": "Testing_Beetl",
                            "l2": "Beetl City",
                            "l3": "School of Beetl",
                            "l4": "1C",
                        },
                        {
                            "l1": "Testing_Beetl",
                            "l2": "Beetl City",
                            "l3": "School of Beetl",
                            "l4": "1D",
                        },
                        {
                            "l1": "Testing_Beetl",
                            "l2": "Beetl Municipality",
                            "l3": "Office",
                            "l4": "Cleaning Unit",
                        },
                        {
                            "l1": "Testing_Beetl",
                            "l2": "Beetl Municipality",
                            "l3": "Office",
                            "l4": "Purchasing Unit",
                        },
                        {
                            "l1": "Testing_Beetl",
                            "l2": "Beetl Municipality",
                            "l3": "Water and Sewage",
                            "l4": "Water Unit",
                        },
                        {
                            "l1": "Testing_Beetl",
                            "l2": "Beetl Municipality",
                            "l3": "Water and Sewage",
                            "l4": "Sewage Unit",
                        },
                    ]
                },
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "Organization Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "Organization",
                    "oql_key": "SELECT Organization WHERE (orgpath = 'Top->Hoglandet' OR orgpath LIKE 'Top->Hoglandet->Testing_Beetl%')",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["code"],
                    "comparison_columns": [
                        "name",
                        "orgpath",
                        "code",
                        "status",
                        "parent_id",
                    ],
                    "link_columns": ["parent_code"],
                },
                "comparisonColumns": [
                    {"name": "name", "type": "Utf8"},
                    {"name": "orgpath", "type": "Utf8"},
                    {"name": "code", "type": "Utf8", "unique": True},
                    {"name": "status", "type": "Utf8"},
                    {"name": "parent_code", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "itop.orgtree",
                        "config": {
                            "treeFields": ["l1", "l2", "l3", "l4"],
                            "toplevel": "Hoglandet",
                        },
                    },
                    {
                        "transformer": "strings.set_default",
                        "config": {
                            "inField": "parent_code",
                            "defaultValue": "",
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "parent_id",
                                    "source_comparison_field": "parent_code",
                                    "foreign_class_type": "Organization",
                                    "foreign_comparison_field": "code",
                                    "use_like_operator": False,
                                }
                            ]
                        },
                    }
                ],
            }
        ],
    }


def delete_14_organizations_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {"static": []},
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "Organization Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "Organization",
                    "oql_key": "SELECT Organization WHERE orgpath LIKE 'Top->Hoglandet->Testing_Beetl%'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["code"],
                    "comparison_columns": [
                        "name",
                        "orgpath",
                        "code",
                        "status",
                        "parent_id",
                    ],
                    "link_columns": ["parent_code"],
                },
                "comparisonColumns": [
                    {"name": "name", "type": "Utf8"},
                    {"name": "orgpath", "type": "Utf8"},
                    {"name": "code", "type": "Utf8", "unique": True},
                    {"name": "status", "type": "Utf8"},
                    {"name": "parent_code", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "itop.orgtree",
                        "config": {
                            "treeFields": ["l1", "l2", "l3", "l4"],
                            "toplevel": "Hoglandet",
                        },
                    }
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "parent_id",
                                    "source_comparison_field": "parent_code",
                                    "foreign_class_type": "Organization",
                                    "foreign_comparison_field": "code",
                                    "use_like_operator": False,
                                }
                            ]
                        },
                    }
                ],
            }
        ],
    }


def insert_3_persons_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {
                    "static": [
                        {
                            "name": "John Doe",
                            "l1": "Testing_Beetl",
                            "l2": "Beetl City",
                            "l3": "School of Beetl",
                            "l4": "1A",
                            "email": "johndoe@example.beetl",
                            "phone": "1234567890",
                            "employee_number": "000110101010",
                            "first_name": "John",
                            "last_name": "Doe",
                            "manager": None,
                            "samaccountname": "johdoe001",
                            "person_type": "Personal",
                        },
                        {
                            "name": "Jane Doe",
                            "l1": "Testing_Beetl",
                            "l2": "Beetl City",
                            "l3": "School of Beetl",
                            "l4": "1A",
                            "email": "janedoe@example.beetl",
                            "employee_number": "000210101010",
                            "phone": "1234567890",
                            "first_name": "Jane",
                            "last_name": "Doe",
                            "manager": "CN=johdoe001,OU=Anvandare,OU=Testing_Beetl,OU=HIT,DC=intern,DC=hoglandet,DC=se",
                            "samaccountname": "jandoe001",
                            "person_type": "Personal",
                        },
                        {
                            "name": "Jim Doe",
                            "l1": "Testing_Beetl",
                            "l2": "Beetl City",
                            "l3": "School of Beetl",
                            "l4": "1A",
                            "email": "jimdoe@example.beetl",
                            "employee_number": "000310101010",
                            "phone": "1234567890",
                            "first_name": "Jim",
                            "last_name": "Doe",
                            "manager": None,
                            "samaccountname": "jimdoe001",
                            "person_type": "Student",
                        },
                    ]
                },
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "Organization Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "Person",
                    "oql_key": "SELECT Person WHERE email LIKE '%@example.beetl'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["samaccountname"],
                    "comparison_columns": [
                        "samaccountname",
                        "org_id",
                        "manager_id",
                        "name",
                        "email",
                        "phone",
                        "first_name",
                        "employee_number",
                    ],
                    "link_columns": ["manager_samaccountname", "org_code"],
                },
                "comparisonColumns": [
                    {"name": "samaccountname", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "email", "type": "Utf8"},
                    {"name": "phone", "type": "Utf8"},
                    {"name": "first_name", "type": "Utf8"},
                    {"name": "employee_number", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "itop.orgcode",
                        "config": {
                            "inFields": ["l1", "l2", "l3", "l4"],
                            "outField": "org_code",
                            "toplevel": "Hoglandet",
                        },
                    },
                    {
                        "transformer": "regex.replace",
                        "config": {
                            "query": "\\D",
                            "replace": "",
                            "inField": "phone",
                            "outField": "phone",
                        },
                    },
                    {
                        "transformer": "misc.sam_from_dn",
                        "config": {
                            "inField": "manager",
                            "outField": "manager_samaccountname",
                        },
                    },
                    {
                        "transformer": "strings.set_default",
                        "config": {
                            "inField": "manager_samaccountname",
                            "defaultValue": "",
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "org_id",
                                    "source_comparison_field": "org_code",
                                    "foreign_class_type": "Organization",
                                    "foreign_comparison_field": "code",
                                    "use_like_operator": False,
                                },
                                {
                                    "source_field": "manager_id",
                                    "source_comparison_field": "manager_samaccountname",
                                    "foreign_class_type": "Person",
                                    "foreign_comparison_field": "samaccountname",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }


def delete_3_persons_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {"static": []},
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "Organization Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "Person",
                    "oql_key": "SELECT Person WHERE email LIKE '%@example.beetl'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["samaccountname"],
                    "comparison_columns": [
                        "samaccountname",
                        "org_id",
                        "manager_id",
                        "name",
                        "email",
                        "phone",
                        "first_name",
                        "employee_number",
                    ],
                    "link_columns": ["manager_samaccountname", "org_code"],
                },
                "comparisonColumns": [
                    {"name": "samaccountname", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "email", "type": "Utf8"},
                    {"name": "phone", "type": "Utf8"},
                    {"name": "first_name", "type": "Utf8"},
                    {"name": "employee_number", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "itop.orgcode",
                        "config": {
                            "inFields": ["l1", "l2", "l3", "l4"],
                            "outField": "org_code",
                            "toplevel": "Hoglandet",
                        },
                    },
                    {
                        "transformer": "regex.replace",
                        "config": {
                            "query": "\\D",
                            "replace": "",
                            "inField": "phone",
                            "outField": "phone",
                        },
                    },
                    {
                        "transformer": "misc.sam_from_dn",
                        "config": {
                            "inField": "manager",
                            "outField": "manager_samaccountname",
                        },
                    },
                    {
                        "transformer": "strings.set_default",
                        "config": {
                            "inField": "manager_samaccountname",
                            "defaultValue": "",
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "org_id",
                                    "source_comparison_field": "org_code",
                                    "foreign_class_type": "Organization",
                                    "foreign_comparison_field": "code",
                                    "use_like_operator": False,
                                },
                                {
                                    "source_field": "manager_id",
                                    "source_comparison_field": "manager_samaccountname",
                                    "foreign_class_type": "Person",
                                    "foreign_comparison_field": "samaccountname",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }


def insert_2_nutanix_clusters_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {
                    "static": [
                        {
                            "uuid": "00000000-0000-0000-0000-000000000001",
                            "name": "Testing_Beetl_Nutanix_Cluster1",
                            "business_criticity": "high",
                            "org_name": "Testing_Beetl",
                        },
                        {
                            "uuid": "00000000-0000-0000-0000-000000000002",
                            "name": "Testing_Beetl_Nutanix_Cluster2",
                            "business_criticity": "high",
                            "org_name": "Testing_Beetl",
                        },
                    ]
                },
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "Organization Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "NutanixCluster",
                    "oql_key": "SELECT NutanixCluster WHERE name LIKE 'Testing_Beetl_Nutanix_Cluster%'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["uuid"],
                    "comparison_columns": ["name", "business_criticity", "org_id"],
                    "link_columns": ["org_code"],
                },
                "comparisonColumns": [
                    {"name": "uuid", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "business_criticity", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "itop.orgcode",
                        "config": {
                            "inFields": ["org_name"],
                            "outField": "org_code",
                            "toplevel": "Hoglandet",
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "org_id",
                                    "source_comparison_field": "org_code",
                                    "foreign_class_type": "Organization",
                                    "foreign_comparison_field": "code",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }


def update_2_nutanix_clusters_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {
                    "static": [
                        {
                            "uuid": "00000000-0000-0000-0000-000000000001",
                            "name": "Testing_Beetl_Nutanix_Cluster3",
                            "business_criticity": "high",
                            "org_name": "Testing_Beetl",
                        },
                        {
                            "uuid": "00000000-0000-0000-0000-000000000002",
                            "name": "Testing_Beetl_Nutanix_Cluster4",
                            "business_criticity": "high",
                            "org_name": "Testing_Beetl",
                        },
                    ]
                },
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "Organization Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "NutanixCluster",
                    "oql_key": "SELECT NutanixCluster WHERE name LIKE 'Testing_Beetl_Nutanix_Cluster%'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["uuid"],
                    "comparison_columns": ["name", "business_criticity", "org_id"],
                    "link_columns": ["org_code"],
                },
                "comparisonColumns": [
                    {"name": "uuid", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "business_criticity", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "itop.orgcode",
                        "config": {
                            "inFields": ["org_name"],
                            "outField": "org_code",
                            "toplevel": "Hoglandet",
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "org_id",
                                    "source_comparison_field": "org_code",
                                    "foreign_class_type": "Organization",
                                    "foreign_comparison_field": "code",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }


def delete_2_nutanix_clusters_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {"static": []},
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "Organization Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "NutanixCluster",
                    "oql_key": "SELECT NutanixCluster WHERE name LIKE 'Testing_Beetl_Nutanix_Cluster%'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["uuid"],
                    "comparison_columns": ["name", "business_criticity", "org_id"],
                    "link_columns": ["org_code"],
                },
                "comparisonColumns": [
                    {"name": "uuid", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "business_criticity", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "itop.orgcode",
                        "config": {
                            "inFields": ["org_name"],
                            "outField": "org_code",
                            "toplevel": "Hoglandet",
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "org_id",
                                    "source_comparison_field": "org_code",
                                    "foreign_class_type": "Organization",
                                    "foreign_comparison_field": "code",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }


def insert_2_nutanix_cluster_hosts_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {
                    "static": [
                        {
                            "uuid": "00000000-0000-0000-0000-000000000001",
                            "cluster_uuid": "00000000-0000-0000-0000-000000000001",
                            "name": "Testing_Beetl_Nutanix_Cluster_Host1",
                            "ip_addr": "127.0.0.1",
                            "controller_vm_ip": "127.0.0.1",
                            "org_name": "Testing_Beetl",
                        },
                        {
                            "uuid": "00000000-0000-0000-0000-000000000002",
                            "cluster_uuid": "00000000-0000-0000-0000-000000000002",
                            "name": "Testing_Beetl_Nutanix_Cluster_Host2",
                            "ip_addr": "127.0.0.1",
                            "controller_vm_ip": "127.0.0.1",
                            "org_name": "Testing_Beetl",
                        },
                    ]
                },
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "Organization Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "NutanixClusterHost",
                    "oql_key": "SELECT NutanixClusterHost WHERE name LIKE 'Testing_Beetl_Nutanix_Cluster_Host%'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["uuid"],
                    "comparison_columns": [
                        "name",
                        "ip_addr",
                        "controller_vm_ip",
                        "cluster_id",
                        "org_id",
                    ],
                    "link_columns": ["cluster_uuid", "org_code"],
                },
                "comparisonColumns": [
                    {"name": "uuid", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "ip_addr", "type": "Utf8"},
                    {"name": "controller_vm_ip", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "strings.set_default",
                        "config": {
                            "inField": "ip_addr",
                            "defaultValue": " ",
                        },
                    },
                    {
                        "transformer": "itop.orgcode",
                        "config": {
                            "inFields": ["org_name"],
                            "outField": "org_code",
                            "toplevel": "Hoglandet",
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "cluster_id",
                                    "source_comparison_field": "cluster_uuid",
                                    "foreign_class_type": "NutanixCluster",
                                    "foreign_comparison_field": "uuid",
                                    "use_like_operator": False,
                                },
                                {
                                    "source_field": "org_id",
                                    "source_comparison_field": "org_code",
                                    "foreign_class_type": "Organization",
                                    "foreign_comparison_field": "code",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }


def update_2_nutanix_cluster_hosts_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {
                    "static": [
                        {
                            "uuid": "00000000-0000-0000-0000-000000000001",
                            "cluster_uuid": "00000000-0000-0000-0000-000000000001",
                            "name": "Testing_Beetl_Nutanix_Cluster_Host3",
                            "ip_addr": "127.0.0.1",
                            "controller_vm_ip": "127.0.0.1",
                            "org_name": "Testing_Beetl",
                        },
                        {
                            "uuid": "00000000-0000-0000-0000-000000000002",
                            "cluster_uuid": "00000000-0000-0000-0000-000000000002",
                            "name": "Testing_Beetl_Nutanix_Cluster_Host4",
                            "ip_addr": "127.0.0.1",
                            "controller_vm_ip": "127.0.0.1",
                            "org_name": "Testing_Beetl",
                        },
                    ]
                },
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "Organization Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "NutanixClusterHost",
                    "oql_key": "SELECT NutanixClusterHost WHERE name LIKE 'Testing_Beetl_Nutanix_Cluster_Host%'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["uuid"],
                    "comparison_columns": [
                        "name",
                        "ip_addr",
                        "controller_vm_ip",
                        "cluster_id",
                        "org_id",
                    ],
                    "link_columns": ["cluster_uuid", "org_code"],
                },
                "comparisonColumns": [
                    {"name": "uuid", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "ip_addr", "type": "Utf8"},
                    {"name": "controller_vm_ip", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "strings.set_default",
                        "config": {
                            "inField": "ip_addr",
                            "defaultValue": " ",
                        },
                    },
                    {
                        "transformer": "itop.orgcode",
                        "config": {
                            "inFields": ["org_name"],
                            "outField": "org_code",
                            "toplevel": "Hoglandet",
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "cluster_id",
                                    "source_comparison_field": "cluster_uuid",
                                    "foreign_class_type": "NutanixCluster",
                                    "foreign_comparison_field": "uuid",
                                    "use_like_operator": False,
                                },
                                {
                                    "source_field": "org_id",
                                    "source_comparison_field": "org_code",
                                    "foreign_class_type": "Organization",
                                    "foreign_comparison_field": "code",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }


def delete_2_nutanix_cluster_hosts_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {"static": []},
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "Organization Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "NutanixClusterHost",
                    "oql_key": "SELECT NutanixClusterHost WHERE name LIKE 'Testing_Beetl_Nutanix_Cluster_Host%'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["uuid"],
                    "comparison_columns": [
                        "name",
                        "ip_addr",
                        "controller_vm_ip",
                        "cluster_id",
                        "org_id",
                    ],
                    "link_columns": ["cluster_uuid", "org_code"],
                },
                "comparisonColumns": [
                    {"name": "uuid", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "ip_addr", "type": "Utf8"},
                    {"name": "controller_vm_ip", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "strings.set_default",
                        "config": {
                            "inField": "ip_addr",
                            "defaultValue": " ",
                        },
                    },
                    {
                        "transformer": "itop.orgcode",
                        "config": {
                            "inFields": ["org_name"],
                            "outField": "org_code",
                            "toplevel": "Hoglandet",
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "cluster_id",
                                    "source_comparison_field": "cluster_uuid",
                                    "foreign_class_type": "NutanixCluster",
                                    "foreign_comparison_field": "uuid",
                                    "use_like_operator": False,
                                },
                                {
                                    "source_field": "org_id",
                                    "source_comparison_field": "org_code",
                                    "foreign_class_type": "Organization",
                                    "foreign_comparison_field": "code",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }


def insert_2_nutanix_networks_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {
                    "static": [
                        {
                            "uuid": "00000000-0000-0000-0000-000000000001",
                            "cluster_uuid": "00000000-0000-0000-0000-000000000001",
                            "name": "Testing_Beetl_Nutanix_Network1",
                            "vlan": "100",
                            "vswitch": "br1",
                            "org_name": "Testing_Beetl",
                        },
                        {
                            "uuid": "00000000-0000-0000-0000-000000000002",
                            "cluster_uuid": "00000000-0000-0000-0000-000000000002",
                            "name": "Testing_Beetl_Nutanix_Network2",
                            "vlan": "200",
                            "vswitch": "br2",
                            "org_name": "Testing_Beetl",
                        },
                    ]
                },
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "Organization Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "NutanixNetwork",
                    "oql_key": "SELECT NutanixNetwork WHERE name LIKE 'Testing_Beetl_Nutanix_Network%'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["uuid"],
                    "comparison_columns": [
                        "name",
                        "vlan",
                        "vswitch",
                        "cluster_id",
                        "org_id",
                    ],
                    "link_columns": ["cluster_uuid", "org_code"],
                },
                "comparisonColumns": [
                    {"name": "uuid", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "vlan", "type": "Utf8"},
                    {"name": "vswitch", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "itop.orgcode",
                        "config": {
                            "inFields": ["org_name"],
                            "outField": "org_code",
                            "toplevel": "Hoglandet",
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "cluster_id",
                                    "source_comparison_field": "cluster_uuid",
                                    "foreign_class_type": "NutanixCluster",
                                    "foreign_comparison_field": "uuid",
                                    "use_like_operator": False,
                                },
                                {
                                    "source_field": "org_id",
                                    "source_comparison_field": "org_code",
                                    "foreign_class_type": "Organization",
                                    "foreign_comparison_field": "code",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }


def update_2_nutanix_networks_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {
                    "static": [
                        {
                            "uuid": "00000000-0000-0000-0000-000000000001",
                            "cluster_uuid": "00000000-0000-0000-0000-000000000001",
                            "name": "Testing_Beetl_Nutanix_Network3",
                            "vlan": "100",
                            "vswitch": "br1",
                            "org_name": "Testing_Beetl",
                        },
                        {
                            "uuid": "00000000-0000-0000-0000-000000000002",
                            "cluster_uuid": "00000000-0000-0000-0000-000000000002",
                            "name": "Testing_Beetl_Nutanix_Network4",
                            "vlan": "200",
                            "vswitch": "br2",
                            "org_name": "Testing_Beetl",
                        },
                    ]
                },
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "Organization Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "NutanixNetwork",
                    "oql_key": "SELECT NutanixNetwork WHERE name LIKE 'Testing_Beetl_Nutanix_Network%'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["uuid"],
                    "comparison_columns": [
                        "name",
                        "vlan",
                        "vswitch",
                        "cluster_id",
                        "org_id",
                    ],
                    "link_columns": ["cluster_uuid", "org_code"],
                },
                "comparisonColumns": [
                    {"name": "uuid", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "vlan", "type": "Utf8"},
                    {"name": "vswitch", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "itop.orgcode",
                        "config": {
                            "inFields": ["org_name"],
                            "outField": "org_code",
                            "toplevel": "Hoglandet",
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "cluster_id",
                                    "source_comparison_field": "cluster_uuid",
                                    "foreign_class_type": "NutanixCluster",
                                    "foreign_comparison_field": "uuid",
                                    "use_like_operator": False,
                                },
                                {
                                    "source_field": "org_id",
                                    "source_comparison_field": "org_code",
                                    "foreign_class_type": "Organization",
                                    "foreign_comparison_field": "code",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }


def delete_2_nutanix_networks_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {"static": []},
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "Organization Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "NutanixNetwork",
                    "oql_key": "SELECT NutanixNetwork WHERE name LIKE 'Testing_Beetl_Nutanix_Network%'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["uuid"],
                    "comparison_columns": [
                        "name",
                        "vlan",
                        "vswitch",
                        "cluster_id",
                        "org_id",
                    ],
                    "link_columns": ["cluster_uuid", "org_code"],
                },
                "comparisonColumns": [
                    {"name": "uuid", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "vlan", "type": "Utf8"},
                    {"name": "vswitch", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "itop.orgcode",
                        "config": {
                            "inFields": ["org_name"],
                            "outField": "org_code",
                            "toplevel": "Hoglandet",
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "cluster_id",
                                    "source_comparison_field": "cluster_uuid",
                                    "foreign_class_type": "NutanixCluster",
                                    "foreign_comparison_field": "uuid",
                                    "use_like_operator": False,
                                },
                                {
                                    "source_field": "org_id",
                                    "source_comparison_field": "org_code",
                                    "foreign_class_type": "Organization",
                                    "foreign_comparison_field": "code",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }


def insert_2_nutanix_virtual_machines_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {
                    "static": [
                        {
                            "name": "Testing_Beetl_Virtual_Machine1",
                            "uuid": "00000000-0000-0000-0000-000000000001",
                            "cluster_uuid": "00000000-0000-0000-0000-000000000001",
                            "org_names": "Testing_Beetl,Beetl City",
                            "ip_addr": "127.0.0.1",
                            "threads_per_core": "1",
                            "vcpu_per_socket": "1",
                            "num_sockets": "1",
                            "memory_mb": "4096",
                            "top_org_name": "Hoglandet",
                            "status": "active",
                        },
                        {
                            "name": "Testing_Beetl_Virtual_Machine2",
                            "uuid": "00000000-0000-0000-0000-000000000002",
                            "cluster_uuid": "00000000-0000-0000-0000-000000000002",
                            "org_names": "Testing_Beetl",
                            "ip_addr": "127.0.0.1",
                            "threads_per_core": "1",
                            "vcpu_per_socket": "1",
                            "num_sockets": "1",
                            "memory_mb": "4096",
                            "top_org_name": "Hoglandet",
                            "status": "active",
                        },
                    ]
                },
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "Organization Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "NutanixVM",
                    "oql_key": "SELECT NutanixVM WHERE name LIKE 'Testing_Beetl_Virtual_Machine%'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["uuid"],
                    "comparison_columns": [
                        "name",
                        "ip_addr",
                        "threads_per_core",
                        "vcpu_per_socket",
                        "num_sockets",
                        "status",
                        "memory_mb",
                        "cluster_id",
                        "org_id",
                    ],
                    "link_columns": ["cluster_uuid", "org_code"],
                },
                "comparisonColumns": [
                    {"name": "uuid", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "ip_addr", "type": "Utf8"},
                    {"name": "threads_per_core", "type": "Utf8"},
                    {"name": "vcpu_per_socket", "type": "Utf8"},
                    {"name": "num_sockets", "type": "Utf8"},
                    {"name": "status", "type": "Utf8"},
                    {"name": "memory_mb", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "strings.uppercase",
                        "config": {
                            "inField": "name",
                            "outField": "name",
                        },
                    },
                    {
                        "transformer": "strings.set_default",
                        "config": {
                            "inField": "org_names",
                            "defaultValue": "Datacenter",
                        },
                    },
                    {
                        "transformer": "strings.split",
                        "config": {
                            "inField": "org_names",
                            "outFields": ["org_name", "org_name_rest"],
                            "separator": ",",
                        },
                    },
                    {
                        "transformer": "itop.orgcode",
                        "config": {
                            "inFields": ["top_org_name", "org_name"],
                            "outField": "org_code",
                            "toplevel": None,
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "cluster_id",
                                    "source_comparison_field": "cluster_uuid",
                                    "foreign_class_type": "NutanixCluster",
                                    "foreign_comparison_field": "uuid",
                                    "use_like_operator": False,
                                },
                                {
                                    "source_field": "org_id",
                                    "source_comparison_field": "org_code",
                                    "foreign_class_type": "Organization",
                                    "foreign_comparison_field": "code",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }


def update_2_nutanix_virtual_machines_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {
                    "static": [
                        {
                            "name": "Testing_Beetl_Virtual_Machine3",
                            "uuid": "00000000-0000-0000-0000-000000000001",
                            "cluster_uuid": "00000000-0000-0000-0000-000000000001",
                            "org_names": "Testing_Beetl,Beetl City",
                            "ip_addr": "127.0.0.1",
                            "threads_per_core": "1",
                            "vcpu_per_socket": "1",
                            "num_sockets": "1",
                            "memory_mb": "4096",
                            "top_org_name": "Hoglandet",
                            "status": "active",
                        },
                        {
                            "name": "Testing_Beetl_Virtual_Machine4",
                            "uuid": "00000000-0000-0000-0000-000000000002",
                            "cluster_uuid": "00000000-0000-0000-0000-000000000002",
                            "org_names": "Testing_Beetl",
                            "ip_addr": "127.0.0.1",
                            "threads_per_core": "1",
                            "vcpu_per_socket": "1",
                            "num_sockets": "1",
                            "memory_mb": "4096",
                            "top_org_name": "Hoglandet",
                            "status": "active",
                        },
                    ]
                },
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "Organization Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "NutanixVM",
                    "oql_key": "SELECT NutanixVM WHERE name LIKE 'Testing_Beetl_Virtual_Machine%'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["uuid"],
                    "comparison_columns": [
                        "name",
                        "ip_addr",
                        "threads_per_core",
                        "vcpu_per_socket",
                        "num_sockets",
                        "status",
                        "memory_mb",
                        "cluster_id",
                        "org_id",
                    ],
                    "link_columns": ["cluster_uuid", "org_code"],
                },
                "comparisonColumns": [
                    {"name": "uuid", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "ip_addr", "type": "Utf8"},
                    {"name": "threads_per_core", "type": "Utf8"},
                    {"name": "vcpu_per_socket", "type": "Utf8"},
                    {"name": "num_sockets", "type": "Utf8"},
                    {"name": "status", "type": "Utf8"},
                    {"name": "memory_mb", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "strings.uppercase",
                        "config": {
                            "inField": "name",
                            "outField": "name",
                        },
                    },
                    {
                        "transformer": "strings.set_default",
                        "config": {
                            "inField": "org_names",
                            "defaultValue": "Datacenter",
                        },
                    },
                    {
                        "transformer": "strings.split",
                        "config": {
                            "inField": "org_names",
                            "outFields": ["org_name", "org_name_rest"],
                            "separator": ",",
                        },
                    },
                    {
                        "transformer": "itop.orgcode",
                        "config": {
                            "inFields": ["top_org_name", "org_name"],
                            "outField": "org_code",
                            "toplevel": None,
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "cluster_id",
                                    "source_comparison_field": "cluster_uuid",
                                    "foreign_class_type": "NutanixCluster",
                                    "foreign_comparison_field": "uuid",
                                    "use_like_operator": False,
                                },
                                {
                                    "source_field": "org_id",
                                    "source_comparison_field": "org_code",
                                    "foreign_class_type": "Organization",
                                    "foreign_comparison_field": "code",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }


def delete_2_nutanix_virtual_machines_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {"static": []},
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "Organization Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "NutanixVM",
                    "oql_key": "SELECT NutanixVM WHERE name LIKE 'Testing_Beetl_Virtual_Machine%'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["uuid"],
                    "comparison_columns": [
                        "name",
                        "ip_addr",
                        "threads_per_core",
                        "vcpu_per_socket",
                        "num_sockets",
                        "status",
                        "memory_mb",
                        "cluster_id",
                        "org_id",
                    ],
                    "link_columns": ["cluster_uuid", "org_code"],
                },
                "comparisonColumns": [
                    {"name": "uuid", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "ip_addr", "type": "Utf8"},
                    {"name": "threads_per_core", "type": "Utf8"},
                    {"name": "vcpu_per_socket", "type": "Utf8"},
                    {"name": "num_sockets", "type": "Utf8"},
                    {"name": "status", "type": "Utf8"},
                    {"name": "memory_mb", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "strings.uppercase",
                        "config": {
                            "inField": "name",
                            "outField": "name",
                        },
                    },
                    {
                        "transformer": "strings.set_default",
                        "config": {
                            "inField": "org_names",
                            "defaultValue": "Datacenter",
                        },
                    },
                    {
                        "transformer": "strings.split",
                        "config": {
                            "inField": "org_names",
                            "outFields": ["org_name", "org_name_rest"],
                            "separator": ",",
                        },
                    },
                    {
                        "transformer": "itop.orgcode",
                        "config": {
                            "inFields": ["top_org_name", "org_name"],
                            "outField": "org_code",
                            "toplevel": None,
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "cluster_id",
                                    "source_comparison_field": "cluster_uuid",
                                    "foreign_class_type": "NutanixCluster",
                                    "foreign_comparison_field": "uuid",
                                    "use_like_operator": False,
                                },
                                {
                                    "source_field": "org_id",
                                    "source_comparison_field": "org_code",
                                    "foreign_class_type": "Organization",
                                    "foreign_comparison_field": "code",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }


def insert_2_nutanix_virtual_machine_nics_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {
                    "static": [
                        {
                            "uuid": "00000000-0000-0000-0000-000000000001",
                            "mac_addr": "00:00:00:00:00:01",
                            "ip_addr": "127.0.0.1",
                            "vlan_uuid": "00000000-0000-0000-0000-000000000001",
                            "vm_uuid": "00000000-0000-0000-0000-000000000001",
                            "vm_name": "Testing_Beetl_Virtual_Machine1",
                        },
                        {
                            "uuid": "00000000-0000-0000-0000-000000000002",
                            "mac_addr": "00:00:00:00:00:02",
                            "ip_addr": "127.0.0.1",
                            "vlan_uuid": "00000000-0000-0000-0000-000000000002",
                            "vm_uuid": "00000000-0000-0000-0000-000000000002",
                            "vm_name": "Testing_Beetl_Virtual_Machine2",
                        },
                    ]
                },
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "iTop Nutanix Virtual Machine NICs Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "NutanixNetworkInterface",
                    "oql_key": "SELECT NutanixNetworkInterface WHERE uuid = '00000000-0000-0000-0000-000000000001' OR uuid = '00000000-0000-0000-0000-000000000002'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["uuid"],
                    "comparison_columns": [
                        "name",
                        "mac_addr",
                        "ip_addr",
                        "vlan_id",
                        "vm_id",
                    ],
                    "link_columns": ["vlan_uuid", "vm_uuid"],
                },
                "comparisonColumns": [
                    {"name": "uuid", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "mac_addr", "type": "Utf8"},
                    {"name": "ip_addr", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "strings.uppercase",
                        "config": {
                            "inField": "mac_addr",
                            "outField": "mac_addr",
                        },
                    },
                    {
                        "transformer": "strings.join",
                        "config": {
                            "inFields": ["ip_addr", "vm_name"],
                            "outField": "name",
                            "separator": "@",
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "vlan_id",
                                    "source_comparison_field": "vlan_uuid",
                                    "foreign_class_type": "NutanixNetwork",
                                    "foreign_comparison_field": "uuid",
                                    "use_like_operator": False,
                                },
                                {
                                    "source_field": "vm_id",
                                    "source_comparison_field": "vm_uuid",
                                    "foreign_class_type": "NutanixVM",
                                    "foreign_comparison_field": "uuid",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }


def update_2_nutanix_virtual_machine_nics_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {
                    "static": [
                        {
                            "uuid": "00000000-0000-0000-0000-000000000001",
                            "mac_addr": "00:00:00:00:00:01",
                            "ip_addr": "127.0.0.2",
                            "vlan_uuid": "00000000-0000-0000-0000-000000000001",
                            "vm_uuid": "00000000-0000-0000-0000-000000000001",
                            "vm_name": "Testing_Beetl_Virtual_Machine1",
                        },
                        {
                            "uuid": "00000000-0000-0000-0000-000000000002",
                            "mac_addr": "00:00:00:00:00:02",
                            "ip_addr": "127.0.0.2",
                            "vlan_uuid": "00000000-0000-0000-0000-000000000002",
                            "vm_uuid": "00000000-0000-0000-0000-000000000002",
                            "vm_name": "Testing_Beetl_Virtual_Machine2",
                        },
                    ]
                },
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "iTop Nutanix Virtual Machine NICs Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "NutanixNetworkInterface",
                    "oql_key": "SELECT NutanixNetworkInterface WHERE uuid = '00000000-0000-0000-0000-000000000001' OR uuid = '00000000-0000-0000-0000-000000000002'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["uuid"],
                    "comparison_columns": [
                        "name",
                        "mac_addr",
                        "ip_addr",
                        "vlan_id",
                        "vm_id",
                    ],
                    "link_columns": ["vlan_uuid", "vm_uuid"],
                },
                "comparisonColumns": [
                    {"name": "uuid", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "mac_addr", "type": "Utf8"},
                    {"name": "ip_addr", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "strings.uppercase",
                        "config": {
                            "inField": "mac_addr",
                            "outField": "mac_addr",
                        },
                    },
                    {
                        "transformer": "strings.join",
                        "config": {
                            "inFields": ["ip_addr", "vm_name"],
                            "outField": "name",
                            "separator": "@",
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "vlan_id",
                                    "source_comparison_field": "vlan_uuid",
                                    "foreign_class_type": "NutanixNetwork",
                                    "foreign_comparison_field": "uuid",
                                    "use_like_operator": False,
                                },
                                {
                                    "source_field": "vm_id",
                                    "source_comparison_field": "vm_uuid",
                                    "foreign_class_type": "NutanixVM",
                                    "foreign_comparison_field": "uuid",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }


def delete_2_nutanix_virtual_machine_nics_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {"static": []},
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "iTop Nutanix Virtual Machine NICs Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "NutanixNetworkInterface",
                    "oql_key": "SELECT NutanixNetworkInterface WHERE uuid = '00000000-0000-0000-0000-000000000001' OR uuid = '00000000-0000-0000-0000-000000000002'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["uuid"],
                    "comparison_columns": [
                        "name",
                        "mac_addr",
                        "ip_addr",
                        "vlan_id",
                        "vm_id",
                    ],
                    "link_columns": ["vlan_uuid", "vm_uuid"],
                },
                "comparisonColumns": [
                    {"name": "uuid", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "mac_addr", "type": "Utf8"},
                    {"name": "ip_addr", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "strings.uppercase",
                        "config": {
                            "inField": "mac_addr",
                            "outField": "mac_addr",
                        },
                    },
                    {
                        "transformer": "strings.join",
                        "config": {
                            "inFields": ["ip_addr", "vm_name"],
                            "outField": "name",
                            "separator": "@",
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "vlan_id",
                                    "source_comparison_field": "vlan_uuid",
                                    "foreign_class_type": "NutanixNetwork",
                                    "foreign_comparison_field": "uuid",
                                    "use_like_operator": False,
                                },
                                {
                                    "source_field": "vm_id",
                                    "source_comparison_field": "vm_uuid",
                                    "foreign_class_type": "NutanixVM",
                                    "foreign_comparison_field": "uuid",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }


def insert_2_nutanix_virtual_machine_disks_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {
                    "static": [
                        {
                            "uuid": "00000000-0000-0000-0000-000000000001",
                            "vm_uuid": "00000000-0000-0000-0000-000000000001",
                            "device_type": "DISK",
                            "name": "nvme0@Testing_Beetl_Virtual_Machine1",
                            "size": "81920",
                        },
                        {
                            "uuid": "00000000-0000-0000-0000-000000000002",
                            "vm_uuid": "00000000-0000-0000-0000-000000000002",
                            "device_type": "DISK",
                            "name": "nvme0@Testing_Beetl_Virtual_Machine2",
                            "size": "81920",
                        },
                    ]
                },
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "iTop Nutanix Virtual Machine Disks Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "NutanixVMDisk",
                    "oql_key": "SELECT NutanixVMDisk WHERE name LIKE 'nvme0@Testing_Beetl_Virtual_Machine%'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["uuid"],
                    "comparison_columns": [
                        "name",
                        "size",
                        "device_type",
                        "vm_id",
                    ],
                    "link_columns": ["vm_uuid"],
                },
                "comparisonColumns": [
                    {"name": "uuid", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "size", "type": "Utf8"},
                    {"name": "device_type", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "strings.uppercase",
                        "config": {
                            "inOutMap": {
                                "uuid": "uuid",
                                "vm_uuid": "vm_uuid",
                                "device_type": "device_type",
                            }
                        },
                    },
                    {
                        "transformer": "int.fillna",
                        "config": {
                            "inField": "size",
                            "outField": "size",
                            "value": 1,
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "vm_id",
                                    "source_comparison_field": "vm_uuid",
                                    "foreign_class_type": "NutanixVM",
                                    "foreign_comparison_field": "uuid",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }


def update_2_nutanix_virtual_machine_disks_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {
                    "static": [
                        {
                            "uuid": "00000000-0000-0000-0000-000000000001",
                            "vm_uuid": "00000000-0000-0000-0000-000000000001",
                            "device_type": "DISK",
                            "name": "nvme0@Testing_Beetl_Virtual_Machine1",
                            "size": "1048",
                        },
                        {
                            "uuid": "00000000-0000-0000-0000-000000000002",
                            "vm_uuid": "00000000-0000-0000-0000-000000000002",
                            "device_type": "DISK",
                            "name": "nvme0@Testing_Beetl_Virtual_Machine2",
                            "size": "1048",
                        },
                    ]
                },
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "iTop Nutanix Virtual Machine Disks Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "NutanixVMDisk",
                    "oql_key": "SELECT NutanixVMDisk WHERE name LIKE 'nvme0@Testing_Beetl_Virtual_Machine%'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["uuid"],
                    "comparison_columns": [
                        "name",
                        "size",
                        "device_type",
                        "vm_id",
                    ],
                    "link_columns": ["vm_uuid"],
                },
                "comparisonColumns": [
                    {"name": "uuid", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "size", "type": "Utf8"},
                    {"name": "device_type", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "strings.uppercase",
                        "config": {
                            "inOutMap": {
                                "uuid": "uuid",
                                "vm_uuid": "vm_uuid",
                                "device_type": "device_type",
                            }
                        },
                    },
                    {
                        "transformer": "int.fillna",
                        "config": {
                            "inField": "size",
                            "outField": "size",
                            "value": 1,
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "vm_id",
                                    "source_comparison_field": "vm_uuid",
                                    "foreign_class_type": "NutanixVM",
                                    "foreign_comparison_field": "uuid",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }


def delete_2_nutanix_virtual_machine_disks_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str, soft_delete: bool = True
):
    return {
        "version": "V1",
        "sources": [
            {
                "name": "src",
                "type": "Static",
                "connection": {"static": []},
            },
            {
                "name": "dst",
                "type": "Itop",
                "connection": {
                    "settings": {
                        "host": itop_url,
                        "username": itop_user,
                        "password": itop_pass,
                        "verify_ssl": False,
                    }
                },
            },
        ],
        "sync": [
            {
                "name": "iTop Nutanix Virtual Machine Disks Sync",
                "source": "src",
                "destination": "dst",
                "sourceConfig": {},
                "destinationConfig": {
                    "datamodel": "NutanixVMDisk",
                    "oql_key": "SELECT NutanixVMDisk WHERE name LIKE 'nvme0@Testing_Beetl_Virtual_Machine%'",
                    "soft_delete": {
                        "enabled": soft_delete,
                        "field": "status",
                        "active_value": "active",
                        "inactive_value": "inactive",
                    },
                    "unique_columns": ["uuid"],
                    "comparison_columns": [
                        "name",
                        "size",
                        "device_type",
                        "vm_id",
                    ],
                    "link_columns": ["vm_uuid"],
                },
                "comparisonColumns": [
                    {"name": "uuid", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "size", "type": "Utf8"},
                    {"name": "device_type", "type": "Utf8"},
                ],
                "sourceTransformers": [
                    {
                        "transformer": "strings.uppercase",
                        "config": {
                            "inOutMap": {
                                "uuid": "uuid",
                                "vm_uuid": "vm_uuid",
                                "device_type": "device_type",
                            }
                        },
                    },
                    {
                        "transformer": "int.fillna",
                        "config": {
                            "inField": "size",
                            "outField": "size",
                            "value": 1,
                        },
                    },
                ],
                "insertionTransformers": [
                    {
                        "transformer": "itop.relations",
                        "config": {
                            "field_relations": [
                                {
                                    "source_field": "vm_id",
                                    "source_comparison_field": "vm_uuid",
                                    "foreign_class_type": "NutanixVM",
                                    "foreign_comparison_field": "uuid",
                                    "use_like_operator": False,
                                },
                            ]
                        },
                    }
                ],
            }
        ],
    }
