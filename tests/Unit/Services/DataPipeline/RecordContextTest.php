<?php

declare(strict_types=1);

use Robel\DataPipeline\RecordContext;

it('initializes from raw data correctly', function () {
    $raw = ['first_name' => 'Alice', 'email' => 'alice@example.com'];
    $record = RecordContext::fromRaw($raw, 42, 'user-42');

    expect($record->raw())->toBe($raw)
        ->and($record->data())->toBe($raw)
        ->and($record->index())->toBe(42)
        ->and($record->identifier())->toBe('user-42')
        ->and($record->isValid())->toBeTrue()
        ->and($record->isInvalid())->toBeFalse()
        ->and($record->isDropped())->toBeFalse();
});

it('allows getting and setting data properties', function () {
    $record = RecordContext::fromRaw(['name' => 'Bob'], 1);

    $record->set('name', 'Robert');
    $record->set('meta.role', 'admin');

    expect($record->get('name'))->toBe('Robert')
        ->and($record->get('meta.role'))->toBe('admin')
        ->and($record->get('nonexistent', 'default'))->toBe('default');
});

it('manages validation errors correctly', function () {
    $record = RecordContext::fromRaw(['email' => 'bad-email'], 1);

    expect($record->isValid())->toBeTrue();

    $record->addError('email', 'Invalid email format.');

    expect($record->isValid())->toBeFalse()
        ->and($record->isInvalid())->toBeTrue()
        ->and($record->errors())->toBe([
            'email' => ['Invalid email format.'],
        ]);
});

it('supports dropping records', function () {
    $record = RecordContext::fromRaw(['name' => 'Spam Bot'], 1);

    expect($record->isDropped())->toBeFalse();
    $record->drop();
    expect($record->isDropped())->toBeTrue();
});
