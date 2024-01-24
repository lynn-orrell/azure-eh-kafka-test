package com.example.appdevgbb;

import java.time.Duration;

public class ProducerMetric {
    
        private long _pid;
        private long _threadId;
        private long _totalRequestedSends;
        private long _recordsSent;
        private Duration _duration;
    
        public ProducerMetric(long threadId, long totalRequestedSends, long recordsSent, Duration duration) {
            _pid = ProcessHandle.current().pid();
            _threadId = threadId;
            _totalRequestedSends = totalRequestedSends;
            _recordsSent = recordsSent;
            _duration = duration;
        }
    
        public long getPid() {
            return _pid;
        }

        public long getThreadId() {
            return _threadId;
        }

        public long getTotalRequestedSends() {
            return _totalRequestedSends;
        }
    
        public long getRecordsSent() {
            return _recordsSent;
        }

        public Duration getDuration() {
            return _duration;
        }
}
