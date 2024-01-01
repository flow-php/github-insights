<?php

namespace App\DataMesh\Paths;

enum Layer: string
{
    case RAW = 'raw';
    case CLEAN = 'clean';
    case ENRICHED = 'enriched';
}
