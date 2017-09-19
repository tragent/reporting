/*
 * Copyright 2017 The Mifos Initiative.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mifos.reporting;

import io.mifos.reporting.api.v1.domain.ReportDefinition;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestIncomeStatementReportSpecification extends AbstractReportingSpecificationTest {
    public TestIncomeStatementReportSpecification() {
        super();
    }

    @Test
    public void shouldReturnReportDefinition() {
        final List<ReportDefinition> reportDefinitions = super.testSubject.fetchReportDefinitions("Accounting");
        Assert.assertTrue(
                reportDefinitions.stream().anyMatch(reportDefinition -> reportDefinition.getIdentifier().equals("Incomestatement"))
        );
    }
}
