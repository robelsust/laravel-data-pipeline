<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Tests\Fixtures;

use Illuminate\Database\Eloquent\Factories\Factory;

/** @extends Factory<User> */
class UserFactory extends Factory
{
    protected $model = User::class;

    /** @return array<string, mixed> */
    public function definition(): array
    {
        return ['name' => fake()->name(), 'email' => fake()->unique()->safeEmail(), 'password' => 'test-password'];
    }
}
