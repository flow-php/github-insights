<?php

namespace App\DataMesh\Dataset\Schema;

use Flow\ETL\Row\Schema;

use function Flow\ETL\DSL\{datetime_schema,
    int_schema,
    map_schema,
    schema,
    str_schema,
    struct_element,
    struct_schema,
    struct_type,
    type_int,
    type_integer,
    type_map,
    type_string};

final class CommitSchemaProvider
{
    public function clean(): Schema
    {
        return schema(
            str_schema('sha'),
            str_schema('node_id'),
            int_schema('pr'),
            datetime_schema('date_utc'),
            map_schema(
                'details_stats',
                type_map(
                    type_string(),
                    type_integer(),
                )
            ),
            struct_schema(
                'author',
                struct_type([
                    struct_element('login', type_string()),
                    struct_element('id', type_int()),
                    struct_element('node_id', type_string()),
                    struct_element('avatar_url', type_string()),
                    struct_element('type', type_string()),
                ])
            )
        );
    }
}
