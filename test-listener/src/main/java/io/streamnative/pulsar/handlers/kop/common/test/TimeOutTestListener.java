/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.kop.common.test;

import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.ThreadDumpUtil;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;
import org.testng.internal.thread.ThreadTimeoutException;

/**
 * TestNG test listener which prints full thread dump into System.err
 * in case a test is failed due to timeout.
 */
@Slf4j
public class TimeOutTestListener extends TestListenerAdapter {

    private static void print(String prefix, ITestResult tr) {
        if (tr.getParameters() != null && tr.getParameters().length > 0) {
            log.info("{} {} {}", prefix, tr.getMethod(), Arrays.toString(tr.getParameters()));
        } else {
            log.info("{} {}", prefix, tr.getMethod());
        }
    }

    @Override
    public void onTestStart(ITestResult tr) {
        print("onTestStart", tr);
        super.onTestStart(tr);
    }

    @Override
    public void onTestSuccess(ITestResult tr) {
        print("onTestSuccess", tr);
        super.onTestSuccess(tr);
    }

    @Override
    public void onTestSkipped(ITestResult tr) {
        print("onTestSkipped", tr);
        super.onTestSkipped(tr);
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult tr) {
        print("onTestFailedButWithinSuccessPercentage", tr);
        super.onTestFailedButWithinSuccessPercentage(tr);
    }

    @Override
    public void onTestFailure(ITestResult tr) {
        print("onTestFailure", tr);
        super.onTestFailure(tr);

        if (tr.getThrowable() != null
                && tr.getThrowable() instanceof ThreadTimeoutException) {
            System.err.println("====> TEST TIMED OUT. PRINTING THREAD DUMP. <====");
            System.err.println();
            System.err.print(ThreadDumpUtil.buildThreadDiagnosticString());
        }
    }
}
