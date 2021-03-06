<?php

require_once __DIR__ . '/QlessTest.php';

class QueueTest extends QlessTest
{
    public function testPutAndPop() {
        $queue = new Qless\Queue('testQueue', $this->client);

        $testData = ['performMethod' => 'myPerformMethod', 'payload' => 'otherData'];
        $queue->put(TestWorkerImpl::class, 'jid', $testData);
        $jobs = $queue->pop('worker');
        $this->assertNotEmpty($jobs);
        $this->assertEquals('jid', $jobs[0]->getId());
    }

    public function testPutWithFalseJobIDGeneratesUUID() {
        $queue = new Qless\Queue('testQueue', $this->client);

        $testData = ['performMethod' => 'myPerformMethod', 'payload' => 'otherData'];
        $res      = $queue->put(TestWorkerImpl::class, false, $testData);
        $this->assertRegExp('/^[[:xdigit:]]{8}-([[:xdigit:]]{4}-){3}[[:xdigit:]]{12}/', $res);
    }

    public function testPopWithNoJobs() {
        $queue = new Qless\Queue('testQueue', $this->client);
        $jobs  = $queue->pop('worker');
        $this->assertEmpty($jobs);
    }

    public function testQueueLength() {
        $queue    = new Qless\Queue('testQueue', $this->client);
        $testData = ['performMethod' => 'myPerformMethod', 'payload' => 'otherData'];
        foreach (range(1, 10) as $i) {
            $queue->put(TestWorkerImpl::class, "jid-$i", $testData);
        }
        $len = $queue->length();
        $this->assertEquals(10, $len);
    }

    public function testPoppingMultipleJobs() {
        $queue    = new Qless\Queue('testQueue', $this->client);
        $testData = ['performMethod' => 'myPerformMethod', 'payload' => 'otherData'];
        $jids     = array_map(function ($i) {
            return "jid-$i";
        }, range(1, 10));

        foreach ($jids as $jid) {
            $queue->put(TestWorkerImpl::class, $jid, $testData);
        }

        $results = $queue->pop('worker', 10);

        $numJobs = count($results);
        $this->assertEquals(10, $numJobs);
    }

    public function testCorrectOrderOfPushingAndPoppingJobs() {
        $queue    = new Qless\Queue('testQueue', $this->client);
        $testData = ['performMethod' => 'myPerformMethod', 'payload' => 'otherData'];
        $jids     = array_map(function ($i) {
            return "jid-$i";
        }, range(1, 10));

        foreach ($jids as $jid) {
            $queue->put(TestWorkerImpl::class, $jid, $testData);
        }

        $results = array_map(function () use ($queue) {
            return $queue->pop('worker')[0]->getId();
        }, $jids);

        $this->assertEquals($jids, $results);
    }

    public function testHigherPriorityJobsArePoppedSooner() {
        $queue    = new Qless\Queue('testQueue', $this->client);
        $testData = ['performMethod' => 'myPerformMethod', 'payload' => 'otherData'];
        $jids     = array_map(function ($i) {
            return "jid-$i";
        }, range(1, 10));

        foreach ($jids as $k => $jid) {
            $queue->put(TestWorkerImpl::class, $jid, $testData, 0, 5, true, $k);
        }

        $results = array_map(function () use ($queue) {
            return $queue->pop('worker')[0]->getId();
        }, $jids);

        $this->assertEquals(array_reverse($jids), $results);
    }

    public function testRunningJobIsNotReplaced() {
        $queue = new Qless\Queue('testQueue', $this->client);
        $res   = $queue->put(TestWorkerImpl::class, 'jid-1', []);
        $this->assertEquals('jid-1', $res);
        $queue->pop('worker');
        $res = $queue->put(TestWorkerImpl::class, 'jid-1', [], 0, 5, false);
        $this->assertGreaterThan(0, $res);
    }

    public function testRunningJobIsReplaced() {
        $queue = new Qless\Queue('testQueue', $this->client);
        $res   = $queue->put(TestWorkerImpl::class, 'jid-1', []);
        $this->assertEquals('jid-1', $res);
        $queue->pop('worker');
        $res = $queue->put(TestWorkerImpl::class, 'jid-1', []);
        $this->assertEquals('jid-1', $res);
    }

    public function testPausedQueueDoesNotReturnJobs() {
        $queue = new Qless\Queue('testQueue', $this->client);
        $queue->pause();
        $queue->put(TestWorkerImpl::class, 'jid', ['performMethod' => 'myPerformMethod', 'payload' => 'otherData']);
        $jobs = $queue->pop('worker');
        $this->assertEquals([], $jobs);
    }

    public function testQueueIsNotPausedByDefault() {
        $queue = new Qless\Queue('testQueue', $this->client);
        $val   = $queue->isPaused();
        $this->assertFalse($val);
    }

    public function testQueueCorrectlyReportsIsPaused() {
        $queue = new Qless\Queue('testQueue', $this->client);
        $queue->pause();
        $val = $queue->isPaused();
        $this->assertTrue($val);
    }

    public function testPausedQueueThatIsResumedDoesReturnJobs() {
        $queue = new Qless\Queue('testQueue', $this->client);
        $queue->pause();
        $queue->put(TestWorkerImpl::class, 'jid', ['performMethod' => 'myPerformMethod', 'payload' => 'otherData']);
        $queue->resume();
        $jobs = $queue->pop('worker');
        $this->assertNotEmpty($jobs);
    }

    public function testHighPriorityJobPoppedBeforeLowerPriorityJobs() {
        $queue = new Qless\Queue('testQueue', $this->client);

        $queue->put(TestWorkerImpl::class, 'jid-1', ['performMethod' => 'myPerformMethod', 'payload' => 'otherData']);
        $queue->put(TestWorkerImpl::class, 'jid-2', ['performMethod' => 'myPerformMethod', 'payload' => 'otherData']);
        $queue->put(TestWorkerImpl::class, 'jid-high', ['performMethod' => 'myPerformMethod', 'payload' => 'otherData'], 0, 0, true, 1);
        $queue->put(TestWorkerImpl::class, 'jid-3', ['performMethod' => 'myPerformMethod', 'payload' => 'otherData']);

        $job = $queue->pop('worker')[0];
        $this->assertEquals('jid-high', $job->getId());
    }

    public function testJobWithIntervalIsThrottled() {
        $queue = new Qless\Queue('testQueue', $this->client);

        $queue->put(TestWorkerImpl::class, 'jid-1', [], 0, 5, true, 0, [], 60);
        $job = $queue->pop('worker')[0];
        $job->complete();

        $queue->put(TestWorkerImpl::class, 'jid-1', [], 0, 5, true, 0, [], 60);
        $job = $queue->pop('worker');
        $this->assertEmpty($job);
    }

    #region heartbeat tests

    public function testItUsesGlobalHeartbeatValueWhenNotSet() {
        $this->client->config->set('heartbeat', 10);
        $queue = new Qless\Queue('testQueue', $this->client);

        $this->assertSame(10, $queue->heartbeat);
    }

    public function testItUsesOwnHeartbeatValue() {
        $queue            = new Qless\Queue('testQueue', $this->client);
        $queue->heartbeat = 55;

        $this->assertSame(55, $queue->heartbeat);
    }

    public function testItCanUnsetHeartbeatValueForQueue() {
        $queue            = new Qless\Queue('testQueue', $this->client);
        $queue->heartbeat = 10;
        $this->assertSame(10, $queue->heartbeat);
        unset($queue->heartbeat);
        $this->assertSame(60, $queue->heartbeat);
    }

    #endregion
}
