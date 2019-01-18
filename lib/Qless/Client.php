<?php

namespace Qless;

require_once __DIR__ . '/Lua.php';
require_once __DIR__ . '/Config.php';
require_once __DIR__ . '/Resource.php';
require_once __DIR__ . '/Jobs.php';

/**
 * Call commands in the qless-core lua script
 *
 * @method string put() put(string $worker, string $queue, string $job_identifier, string $klass, string $data, int $delay_in_seconds, mixed ...$args)
 * @method string recur() recur(string $queue, string $jid, string $klass, string $data, string $spec, mixed ...$args)
 * @method string requeue() requeue(string $worker, string $queue, string $job_identifier, string $klass, string $data, int $delay_in_seconds, mixed ...$args)
 * @method string pop() pop(string $queue, string $worker, int $count)
 * @method int length() length(string $queue)
 * @method int heartbeat() heartbeat()
 * @method int retry() retry(string $jid, string $queue, string $worker, int $delay, string $group, string $message)
 * @method int cancel() cancel(string $jid)
 * @method int unrecur() unrecur(string $jid)
 * @method int fail() fail(string $jid, string $worker, string $group, string $message, string $data = null)
 * @method string[] jobs(string $state, int $offset = 0, int $count = 25)
 * @method string get(string $jid)
 * @method string[] multiget(array $jids)
 * @method bool complete(string $jid, string $worker_name, string $queue_name, string $data)
 * @method void timeout(string $jid)
 * @method string failed(string $group = false, int $start = 0, int $limit = 25)
 * @method string[] tag(string $op, $tags)
 * @method array stats(string $name, int $date = null)
 * @method bool paused(string $name)
 * @method void pause(string $name)
 * @method void unpause(string $name)
 *
 * @property-read Jobs jobs
 */
class Client
{
    /**
     * Used for testing and internal use
     *
     * @var Lua
     * @internal
     */
    public $lua;

    /**
     * @var Config
     */
    public $config;

    /**
     * @var array
     */
    private $redis = [];

    /**
     * @var Jobs
     */
    private $_jobs;

    public function __construct($host = 'localhost', $port = 6379) {
        $this->redis['host'] = $host;
        $this->redis['port'] = $port;

        $this->lua    = new Lua($this->redis);
        $this->config = new Config($this);
        $this->_jobs  = new Jobs($this);
    }

    /**
     * Return the host for the Redis server
     *
     * @return string
     */
    public function getRedisHost() {
        return $this->redis['host'];
    }

    /**
     * Return the port for the Redis server
     *
     * @return int
     */
    public function getRedisPort() {
        return $this->redis['port'];
    }

    /**
     * Create a new listener
     *
     * @param $channels
     *
     * @return Listener
     */
    public function createListener($channels) {
        return new Listener($this->redis, $channels);
    }

    public function __call($command, $arguments) {
        return $this->lua->run($command, $arguments);
    }

    public function __get($prop) {
        if ($prop === 'jobs') {
            return $this->_jobs;
        }

        return null;
    }

    public function __set($name, $value) {
    }

    public function __isset($name) {
        return false;
    }

    /**
     * Call a specific qless command
     *
     * @param string $command
     * @param array  $arguments ...
     *
     * @return mixed
     */
    public function call($command, ...$arguments) {
        return $this->lua->run($command, $arguments);
    }

    /**
     * Returns
     *
     * @param string $name name of queue
     *
     * @return Queue
     */
    public function getQueue($name) {
        return new Queue($name, $this);
    }

    /**
     * APIs for manipulating a resource
     *
     * @param $name
     *
     * @return Resource
     */
    public function getResource($name) {
        return new Resource($this, $name);
    }

    /**
     * Returns APIs for querying information about jobs
     *
     * @return Jobs
     */
    public function getJobs() {
        return $this->_jobs;
    }

    /**
     * Call to reconnect to Redis server
     */
    public function reconnect() {
        $this->lua->reconnect();
    }
}
