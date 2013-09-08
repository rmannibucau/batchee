/*
 * Copyright 2012 International Business Machines Corp.
 * 
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package org.apache.batchee.container.impl.controller.chunk;

public class CheckpointDataKey {

    private long _jobInstanceId;

    // OK, this comes from IBM terminology, but I can't come up with a better name so we're leaving it.
    // "readerOrWriterName" ?
    private String _batchDataStreamName;
    private String _stepName;

    public CheckpointDataKey(long jobId, String stepName, String bdsName) {
        this._jobInstanceId = jobId;
        this._stepName = stepName;
        this._batchDataStreamName = bdsName;
    }

    public long getJobInstanceId() {
        return _jobInstanceId;
    }

    public String getBatchDataStreamName() {
        return _batchDataStreamName;
    }

    public String getStepName() {
        return _stepName;
    }

    public String getCommaSeparatedKey() {
        return stringify();
    }

    public String toString() {
        return stringify();
    }

    private String stringify() {
        return _jobInstanceId + "," + _stepName + "," + _batchDataStreamName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final CheckpointDataKey that = CheckpointDataKey.class.cast(o);
        return _jobInstanceId == that._jobInstanceId
            && _batchDataStreamName.equals(that._batchDataStreamName)
            && _stepName.equals(that._stepName);
    }

    @Override
    public int hashCode() {
        int result = (int) (_jobInstanceId ^ (_jobInstanceId >>> 32));
        result = 31 * result + _batchDataStreamName.hashCode();
        result = 31 * result + _stepName.hashCode();
        return result;
    }
}
