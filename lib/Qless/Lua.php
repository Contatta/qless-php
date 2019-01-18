<?php

namespace Qless;

require_once __DIR__ . '/QlessException.php';

/**
 * Wrapper to load and execute lua script for qless-core.
 */
class Lua
{
    /**
     * @var \Redis
     */
    protected $redisCli;

    /**
     * @var string
     */
    private $redisHost;

    /**
     * @var int
     */
    private $redisPort;

    /**
     * @var string
     */
    protected $sha;

    public function __construct($redis) {
        $this->redisHost = $redis['host'];
        $this->redisPort = $redis['port'];

        $this->redisCli = new \Redis();
        $this->redisCli->connect($this->redisHost, $this->redisPort);
    }

    public function run($command, $args) {
        if (empty($this->sha)) {
            $this->reload();
        }
        $luaArgs  = [$command, microtime(true)];
        $argArray = array_merge($luaArgs, $args);
        $result   = $this->redisCli->evalSha($this->sha, $argArray);
        if ($error = $this->redisCli->getLastError()) {
            $this->handleError($error);
            return null;
        }

        return $result;
    }

    /**
     * @param $error
     *
     * @throws QlessException
     */
    protected function handleError($error) {
        $this->redisCli->clearLastError();
        throw QlessException::createExceptionFromError($error);
    }

    protected function reload() {
        $script    = file_get_contents(__DIR__ . '/qless-core/qless.lua', true);
        $this->sha = sha1($script);
        $res       = $this->redisCli->script('exists', $this->sha);
        if ($res[0] !== 1) {
            $this->sha = $this->redisCli->script('load', $script);
        }
    }

    /**
     * Removes all the entries from the default Redis database
     *
     * @internal
     */
    public function flush() {
        $this->redisCli->flushDB();
    }

    /**
     * Reconnect to the Redis server
     */
    public function reconnect() {
        $this->redisCli->close();
        $this->redisCli->connect($this->redisHost, $this->redisPort);
    }
}
