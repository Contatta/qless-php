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
                $config['parameters']['persistent'] = true;
                $options['connections'] = [
                    'tcp'  => 'Predis\Connection\PhpiredisStreamConnection',
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
        if($this->redisConfig['type'] === 'redis') {
            $this->redis->clearLastError();
        }
    }
    protected function evalSha($sha, $command, $args) {
        switch($this->redisConfig['type']) {
            case 'redis':
                $argArray = array_merge([$command, microtime(true)], $args);
                $out = $this->redis->evalSha($sha,$argArray);
                $this->redisError = $this->redis->getLastError();
                return $out;
            case 'predis':
                $args = array_merge(['evalsha',$sha,0,$command,microtime(true)],$args);
                try {
                    $cmd = phpiredis_format_command($args);
                    stream_socket_sendto($this->redis->getConnection()->getResource(),$cmd);
                    $data = $this->redis->getConnection()->read();
                    if($data instanceof \Predis\ResponseError) {
                        $this->redisError = $data->getMessage();
                        return false;
                    }
                    return $data;
                } catch(\Exception $e) {
                    $this->redisError = $e->getMessage();
                    return false;
                }
        }
    }
}