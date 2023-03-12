package in.drongo.drongodb.listener;

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.reporting.ReportEntry;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JunitTestListener implements TestExecutionListener {
	//private static Logger logger = LoggerFactory.getLogger(TestExecutionListener.class);
	
	public JunitTestListener() {
	}
	
	@Override
	public void dynamicTestRegistered(TestIdentifier testIdentifier) {
		try {
		} finally {
			TestExecutionListener.super.dynamicTestRegistered(testIdentifier);
		}
	}
	/**
	 * On every, Class and methods
	 */
	@Override
	public void executionFinished(TestIdentifier testIdentifier, TestExecutionResult testExecutionResult) {
		try {
		} finally {
			TestExecutionListener.super.executionFinished(testIdentifier, testExecutionResult);
		}
	}
	/**
	 * On every, Class and methods
	 */
	@Override
	public void executionSkipped(TestIdentifier testIdentifier, String reason) {
		try {
		} finally {
			TestExecutionListener.super.executionSkipped(testIdentifier, reason);
		}
	}
	/**
	 * On every, Class and methods
	 */
	@Override
	public void executionStarted(TestIdentifier testIdentifier) {
		try {
		} finally {
			TestExecutionListener.super.executionStarted(testIdentifier);
		}
	}

	@Override
	public void reportingEntryPublished(TestIdentifier testIdentifier, ReportEntry entry) {
		try {
		} finally {
			TestExecutionListener.super.reportingEntryPublished(testIdentifier, entry);
		}
	}
	/**
	 * On src/test/java finished
	 */
	@Override
	public void testPlanExecutionFinished(TestPlan testPlan) {
		try {
		} finally {
			TestExecutionListener.super.testPlanExecutionFinished(testPlan);
		}
	}
	/**
	 * On src/test/java started
	 */
	@Override
	public void testPlanExecutionStarted(TestPlan testPlan) {
		try {
		} finally {
			TestExecutionListener.super.testPlanExecutionStarted(testPlan);
		}
	}

}
