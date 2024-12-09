def insert_15_organizations_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str
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
                        "enabled": True,
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


def delete_15_organizations_from_static_to_itop(
    itop_url: str, itop_user: str, itop_pass: str
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
                        "enabled": False,
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
