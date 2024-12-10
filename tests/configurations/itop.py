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
                    # only here to make transformation work
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
                    # only here to make transformation work
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
                    # only here to make transformation work
                    "link_columns": ["manager_samaccountname", "org_code"],
                },
                "comparisonColumns": [
                    {"name": "samaccountname", "type": "Utf8", "unique": True},
                    {"name": "name", "type": "Utf8"},
                    {"name": "email", "type": "Utf8"},
                    {"name": "phone", "type": "Utf8"},
                    {"name": "first_name", "type": "Utf8"},
                    {"name": "employee_number", "type": "Utf8"},
                    # {"name": "manager_id", "type": "Utf8"},
                    # {"name": "org_id", "type": "Utf8"},
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
                    # only here to make transformation work
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
