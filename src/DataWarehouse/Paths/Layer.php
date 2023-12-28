<?php

namespace App\DataWarehouse\Paths;

enum Layer: string
{
    case RAW = 'raw';
    case CLEAN = 'curated';
    case ENRICHED = 'published';
}
