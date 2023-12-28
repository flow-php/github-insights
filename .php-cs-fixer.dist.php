<?php

$finder = (new PhpCsFixer\Finder())
    ->in(__DIR__)
    ->exclude('var')
;

return (new PhpCsFixer\Config())
    ->setRules([
        '@Symfony' => true,
        'single_import_per_statement' => false,
        'group_import' => true,
        'concat_space' => ['spacing' => 'one'],
        'no_unused_imports' => true
    ])
    ->setFinder($finder)
;
