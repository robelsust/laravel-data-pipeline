<?php

declare(strict_types=1);

namespace Robel\DataPipeline\Tests\Fixtures;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Foundation\Auth\User as Authenticatable;

class User extends Authenticatable
{
    /** @use HasFactory<UserFactory> */
    use HasFactory;

    protected $table = 'users';

    protected $fillable = ['name', 'username', 'email', 'password', 'remember_token'];

    protected function casts(): array
    {
        return ['password' => 'hashed'];
    }

    protected static function newFactory(): UserFactory
    {
        return UserFactory::new();
    }
}
