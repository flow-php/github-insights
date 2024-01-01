<?php

namespace App\DataMesh\Dataset\Schema\Clean;

use Flow\ETL\Row\Schema;

use function Flow\ETL\DSL\{bool_schema, datetime_schema, int_schema, list_schema, schema, str_schema, struct_element, struct_schema, struct_type, type_boolean, type_int, type_list, type_string};

final class PullRequestSchemaProvider
{
    public function schema(): Schema
    {
        return schema(
            str_schema('url'),
            int_schema('id'),
            str_schema('node_id'),
            int_schema('number'),
            str_schema('state'),
            bool_schema('locked'),
            str_schema('title'),
            struct_schema(
                'user',
                struct_type(
                    struct_element('login', type_string()),
                    struct_element('id', type_int()),
                    struct_element('node_id', type_string()),
                    struct_element('avatar_url', type_string()),
                    struct_element('url', type_string()),
                    struct_element('type', type_string()),
                    struct_element('site_admin', type_boolean()),
                )
            ),
            str_schema('body', nullable: true),
            datetime_schema('date_utc'),
            datetime_schema('created_at_utc'),
            datetime_schema('updated_at_utc', nullable: true),
            datetime_schema('closed_at_utc', nullable: true),
            datetime_schema('merged_at_utc'),
            list_schema(
                'labels',
                type_list(
                    struct_type(
                        struct_element('id', type_int()),
                        struct_element('node_id', type_string()),
                        struct_element('name', type_string()),
                        struct_element('color', type_string()),
                        struct_element('default', type_boolean()),
                    )
                ),
                nullable: true
            ),
        );
    }
}
