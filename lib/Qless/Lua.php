<?php

namespace Qless;

require_once __DIR__ . '/QlessException.php';

/**
 * Class Lua
 * wrapper to load and execute lua script for qless-core.
 *
 * @package Qless
 */
class Lua
{
    use RedisTrait;


    public function __construct($redisConfig) {
        $this->prepareConnection($redisConfig);
        $this->connect();
    }

    public function run($command, $args) {
        if (empty($this->sha)) {
            $this->reload();
        }
        $luaArgs  = [$command, microtime(true)];
        $argArray = array_merge($luaArgs, $args);
        $result = $this->evalSha($this->sha, $argArray);
        $error  = $this->getLastError();
        if ($error) {
            $this->handleError($error);
            return null;
        }

        return $result;
    }

    protected function handleError($error) {
        $this->clearLastError();
        throw QlessException::createException($error);
    }

    /**
     * Removes all the entries from the default Redis database
     *
     * @internal
     */
    public function flush() {
        $this->redis->flushDB();
    }

}