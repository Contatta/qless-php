<?php

namespace Qless;

class Jobs implements \ArrayAccess
{
    /**
     * @var Client
     */
    private $client;

    public function __construct(Client $client) {
        $this->client = $client;
    }

    /**
     * Return a paginated list of JIDs which are in a completed state
     *
     * @param int $offset
     * @param int $count
     *
     * @return string[]
     */
    public function completed($offset = 0, $count = 25) {
        return $this->client->jobs('complete', $offset, $count);
    }

    /**
     * Return a {@see Job} instance for the specified job identifier or null if the job does not exist
     *
     * @param string $jid the job identifier to fetch
     *
     * @return Job|null
     */
    public function get($jid) {
        return $this->offsetGet($jid);
    }

    /**
     * Returns a list of jobs for the specified job identifiers, keyed by job identifier
     *
     * @param string[] $jids
     *
     * @return Job[]
     */
    public function multiget($jids) {
        if (empty($jids)) {
            return [];
        }

        $jobs = json_decode($this->client->multiget(...$jids), true);
        $ret  = [];
        foreach ($jobs as $job_data) {
            $job                = new Job($this->client, $job_data);
            $ret[$job->getId()] = $job;
        }

        return $ret;
    }

    /**
     * Fetches failed jobs for the specified group, keyed by job identifier
     *
     * @param string $group
     * @param int    $start
     * @param int    $limit
     *
     * @return \Iterator|Job[]
     */
    public function failedForGroup($group, $start = 0, $limit = 25) {
        $results = json_decode($this->client->failed($group, $start, $limit), true);
        if (!empty($results['jobs'])) {
            $results['jobs'] = $this->multiget($results['jobs']);
        }

        return $results;
    }

    /**
     * Fetches the number of failed jobs for each group
     *
     * @return int[]
     */
    public function failed() {
        return json_decode($this->client->failed(), true);
    }

    public function offsetExists($jid) {
        return $this->client->get($jid) !== false;
    }

    public function offsetGet($jid) {
        $job_data = $this->client->get($jid);
        if ($job_data === false) {
            $job_data = $this->client->{'recur.get'}($jid);
            if ($job_data === false) {
                return null;
            }
        }

        return new Job($this->client, json_decode($job_data, true));
    }

    public function offsetSet($offset, $value) {
        throw new \LogicException('set not supported');
    }

    public function offsetUnset($offset) {
        throw new \LogicException('unset not supported');
    }
}
