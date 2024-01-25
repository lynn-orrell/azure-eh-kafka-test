package com.example.appdevgbb;

import java.time.Duration;

public class ConsumerMetric {
    
        private long _pid;
        private long _threadId;
        private long _totalRecordsRead;
        private long _totalRecordsCommitted;
        private long _numRecordsCommitted;
        private long _recordsReadBeforeCommit;
        private long _totalEndToEndLatency;
        private Duration _duration;
    
        public ConsumerMetric(long threadId, long totalRecordsRead, long totalRecordsCommitted, long numRecordsCommitted, long recordsReadBeforeCommit, long totalEndToEndLatency, Duration duration) {
            _pid = ProcessHandle.current().pid();
            _threadId = threadId;
            _totalRecordsRead = totalRecordsRead;
            _totalRecordsCommitted = totalRecordsCommitted;
            _numRecordsCommitted = numRecordsCommitted;
            _recordsReadBeforeCommit = recordsReadBeforeCommit;
            _totalEndToEndLatency = totalEndToEndLatency;
            _duration = duration;
        }
    
        public long getPid() {
            return _pid;
        }

        public long getThreadId() {
            return _threadId;
        }

        public long getTotalRecordsRead() {
            return _totalRecordsRead;
        }

        public void setTotalRecordsRead(long totalRecordsRead) {
            _totalRecordsRead = totalRecordsRead;
        }

        public long getTotalRecordsCommitted() {
            return _totalRecordsCommitted;
        }

        public void setTotalRecordsCommitted(long totalRecordsCommitted) {
            _totalRecordsCommitted = totalRecordsCommitted;
        }

        public long getNumRecordsCommitted() {
            return _numRecordsCommitted;
        }

        public long getRecordsReadBeforeCommit() {
            return _recordsReadBeforeCommit;
        }

        public long getTotalEndToEndLatency() {
            return _totalEndToEndLatency;
        }

        public void setTotalEndToEndLatency(long totalEndToEndLatency) {
            _totalEndToEndLatency = totalEndToEndLatency;
        }

        public Duration getDuration() {
            return _duration;
        }
}
