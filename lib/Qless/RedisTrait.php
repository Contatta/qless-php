<?php
namespace Qless;

trait RedisTrait {
    /** @var  \Redis|\Predis\Client */
    protected $redis;
    protected $redisConfig;

    protected function prepareConnection($config) {
        $this->redisConfig = $config;
    }
    protected function connect() {
        if($this->redisConfig['type'] == 'redis' && class_exists('Redis')) {
            $redis = new \Redis();
            $redis->connect($this->redisConfig['host'], $this->redisConfig['port']);
        } elseif($this->redisConfig['type'] == 'predis') {
            $config =
                ['parameters'=>[
                    'scheme'=>'tcp',
                    'host' => $this->redisConfig['host'],
                    'port' => $this->redisConfig['port'],
                ]];
            $options = [];
            if(extension_loaded('phpiredis')) {
                $options['connections'] = [
                    'tcp'  => 'Predis\Connection\PhpiredisConnection',
                    'unix' => 'Predis\Connection\PhpiredisConnection',
                ];
            }
            $redis = new \Predis\Client($config,$options);
        } else {
            throw QlessException::createException('Could not find class with which to connect a Redis database.');
        }
        $this->redis = $redis;
    }
    /**
     * Reconnect to the Redis server
     */
    public function reconnect() {
        $this->redis->close();
        $this->connect();
    }

    /**
     * @var string
     */
    protected $sha = null;

    protected function reload() {
        $file = __DIR__ . '/qless-core/qless.lua';
        $this->sha = sha1_file($file);
        switch(get_class($this->redis)) {
            case 'Redis':
                $res = $this->redis->script('exists', $this->sha);
                if ($res[0] !== 1) {
                    $this->sha = $this->redis->script('load', file_get_contents($file));
                }
                break;
            case 'Predis\Client':
                $res = $this->redis->script('EXISTS',$this->sha);
                if ($res[0] !== 1) {
                    $this->redis->script('LOAD', file_get_contents($file));
                }
                break;
        }
    }

    private $redisError;
    protected function getLastError() {
        return $this->redisError;
    }
    protected function clearLastError() {
        $this->redisError = null;
        if(get_class($this->redis) === 'Redis') {
            $this->redis->clearLastError();
        }
    }
    protected function evalSha($sha, $argArray) {
        switch(get_class($this->redis)) {
            case 'Redis':
                $out = $this->redis->evalSha($sha,$argArray);
                $this->redisError = $this->redis->getLastError();
                return $out;
            case 'Predis\Client':
                array_unshift($argArray,0);
                array_unshift($argArray,$sha);
                try {
                    return call_user_func_array([$this->redis,'evalSha'],$argArray);
                } catch(\Exception $e) {
                    $this->redisError = $e->getMessage();
                    return false;
                }
        }
    }
}