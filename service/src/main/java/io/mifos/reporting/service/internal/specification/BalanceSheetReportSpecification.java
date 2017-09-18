package io.mifos.reporting.service.internal.specification;

import io.mifos.core.api.util.UserContextHolder;
import io.mifos.core.lang.DateConverter;
import io.mifos.reporting.api.v1.domain.*;
import io.mifos.reporting.service.ServiceConstants;
import io.mifos.reporting.service.spi.*;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@Report(category = "Accounting", identifier = "Balancesheet")
public class BalanceSheetReportSpecification implements ReportSpecification {

    private static final String ID = "Id";
    private static final String IDENTIFIER = "Identifier";
    private static final String LEDGER = "Ledger";

    private static final String PARENT_LEDGER = "Parent Ledger";
    private static final String ACCOUNT_IDENFIFIER = "Account Identifier";
    private static final String ACCOUNT_NAME = "Account Name";
    private static final String ACCOUNT_BALANCE = "Account Balance";

    private final Logger logger;

    private final EntityManager entityManager;

    private final HashMap<String, String> ledgerColumnMapping = new HashMap<>();
    private final HashMap<String, String> accountColumnMapping = new HashMap<>();
    private final HashMap<String, String> allColumnMapping = new HashMap<>();

    @Autowired
    public BalanceSheetReportSpecification(@Qualifier(ServiceConstants.LOGGER_NAME) final Logger logger,
                                           final EntityManager entityManager) {
        super();
        this.logger = logger;
        this.entityManager = entityManager;
        this.initializeMapping();
    }

    private void initializeMapping() {
        this.ledgerColumnMapping.put(ID, "ledger.id");
        this.ledgerColumnMapping.put(IDENTIFIER, "ledger.identifier");
        this.ledgerColumnMapping.put(LEDGER, "ledger.description");

        this.accountColumnMapping.put(PARENT_LEDGER, "acc.ledger_id");
        this.accountColumnMapping.put(ACCOUNT_IDENFIFIER, "acc.identifier");
        this.accountColumnMapping.put(ACCOUNT_NAME, "acc.a_name");
        this.accountColumnMapping.put(ACCOUNT_BALANCE, "acc.balance");

        this.allColumnMapping.putAll(ledgerColumnMapping);
        this.allColumnMapping.putAll(accountColumnMapping);

    }

    @Override
    public ReportDefinition getReportDefinition() {
        final ReportDefinition reportDefinition = new ReportDefinition();
        reportDefinition.setIdentifier("Balancesheet");
        reportDefinition.setName("Balance Sheet");
        reportDefinition.setDescription("Balance sheet report");
        reportDefinition.setQueryParameters(this.buildQueryParameters());
        reportDefinition.setDisplayableFields(this.buildDisplayableFields());
        return reportDefinition;
    }

    private List<DisplayableField> buildDisplayableFields() {
        return Arrays.asList(
                DisplayableFieldBuilder.create(ID, Type.TEXT).mandatory().build(),
                DisplayableFieldBuilder.create(IDENTIFIER, Type.TEXT).mandatory().build(),
                DisplayableFieldBuilder.create(LEDGER, Type.TEXT).mandatory().build(),

                DisplayableFieldBuilder.create(ACCOUNT_IDENFIFIER, Type.TEXT).mandatory().build(),
                DisplayableFieldBuilder.create(ACCOUNT_NAME, Type.TEXT).mandatory().build(),
                DisplayableFieldBuilder.create(ACCOUNT_BALANCE, Type.TEXT).mandatory().build()
        );    }

    private List<QueryParameter> buildQueryParameters() {
        return Arrays.asList();
    }

    @Override
    public ReportPage generateReport(final ReportRequest reportRequest, final int pageIndex, final int size) {
        final ReportDefinition reportDefinition = this.getReportDefinition();
        this.logger.info("Generating report {0}.", reportDefinition.getIdentifier());

        final ReportPage reportPage = new ReportPage();
        reportPage.setName(reportDefinition.getName());
        reportPage.setDescription(reportDefinition.getDescription());
        reportPage.setHeader(this.createHeader(reportRequest.getDisplayableFields()));

        final Query customerQuery = this.entityManager.createNativeQuery(this.buildLedgerQuery(reportRequest, pageIndex, size));
        final List<?> customerResultList = customerQuery.getResultList();
        reportPage.setRows(this.buildRows(reportRequest, customerResultList));

        reportPage.setHasMore(
                !this.entityManager.createNativeQuery(this.buildLedgerQuery(reportRequest, pageIndex + 1, size))
                        .getResultList().isEmpty()
        );

        reportPage.setGeneratedBy(UserContextHolder.checkedGetUser());
        reportPage.setGeneratedOn(DateConverter.toIsoString(LocalDateTime.now(Clock.systemUTC())));
        return reportPage;
    }

    @Override
    public void validate(final ReportRequest reportRequest) throws IllegalArgumentException {
        final ArrayList<String> unknownFields = new ArrayList<>();
        reportRequest.getQueryParameters().forEach(queryParameter -> {
            if (!this.allColumnMapping.keySet().contains(queryParameter.getName())) {
                unknownFields.add(queryParameter.getName());
            }
        });

        reportRequest.getDisplayableFields().forEach(displayableField -> {
            if (!this.allColumnMapping.keySet().contains(displayableField.getName())) {
                unknownFields.add(displayableField.getName());
            }
        });

        if (!unknownFields.isEmpty()) {
            throw new IllegalArgumentException(
                    "Unspecified fields requested: " + unknownFields.stream().collect(Collectors.joining(", "))
            );
        }
    }

    private Header createHeader(final List<DisplayableField> displayableFields) {
        final Header header = new Header();
        header.setColumnNames(
                displayableFields
                        .stream()
                        .map(DisplayableField::getName)
                        .collect(Collectors.toList())
        );
        return header;
    }


    private List<Row> buildRows(final ReportRequest reportRequest, final List<?> ledgerResultList) {
        final ArrayList<Row> rows = new ArrayList<>();

        ledgerResultList.forEach(result -> {
            final Row row = new Row();
            row.setValues(new ArrayList<>());

            final String ledgerIdentifier;

            if (result instanceof Object[]) {
                final Object[] resultValues = (Object[]) result;

                ledgerIdentifier = resultValues[0].toString();

                for (final Object resultValue : resultValues) {
                    final Value value = new Value();
                    if (resultValue != null) {
                        value.setValues(new String[]{resultValue.toString()});
                    } else {
                        value.setValues(new String[]{});
                    }

                    row.getValues().add(value);
                }
            } else {
                ledgerIdentifier = result.toString();

                final Value value = new Value();
                value.setValues(new String[]{result.toString()});
                row.getValues().add(value);
            }

            final Query subLedgerQuery = this.entityManager.createNativeQuery(this.buildSubLedgerQuery(reportRequest, ledgerIdentifier));
            final List<?> subLedgerResultList = subLedgerQuery.getResultList();
            final ArrayList<String> values = new ArrayList<>();
            subLedgerResultList.forEach(subLedgerResult -> {
                if (subLedgerResult instanceof Object[]) {
                    final Object[] subLedgerResultValues = (Object[]) subLedgerResult;

                    final String parentLedgerIdentifier = subLedgerResultValues[0].toString();

                    final String subLedgerValue = subLedgerResultValues[0].toString() + " "
                            + subLedgerResultValues[1].toString() + " " + subLedgerResultValues[2];
                    values.add(subLedgerValue);

                    final String accountQueryString = this.buildAccountQuery(reportRequest, parentLedgerIdentifier);

                    final Query accountQuery = this.entityManager.createNativeQuery(accountQueryString);

                    final List<?> accountResultList = accountQuery.getResultList();

                    accountResultList.forEach(accountResult -> {

                        if (accountResult instanceof Object[]) {
                            final Object[] resultValues = (Object[]) accountResult;

                            for (final Object resultValue : resultValues) {
                                final Value value = new Value();
                                if (resultValue != null) {
                                    value.setValues(new String[]{resultValue.toString()});
                                } else {
                                    value.setValues(new String[]{});
                                }

                                row.getValues().add(value);
                            }
                        } else {
                            final Value value = new Value();
                            value.setValues(new String[]{accountResult.toString()});
                            row.getValues().add(value);
                        }

                    });


                    final Value subLedgerVal = new Value();
                    subLedgerVal.setValues(values.toArray(new String[values.size()]));
                    row.getValues().add(subLedgerVal);
                }
            });

            rows.add(row);
        });

        return rows;
    }

    private String buildLedgerQuery(final ReportRequest reportRequest, int pageIndex, int size) {
        final StringBuilder query = new StringBuilder("SELECT ");

        final List<DisplayableField> displayableFields = reportRequest.getDisplayableFields();
        final ArrayList<String> columns = new ArrayList<>();
        displayableFields.forEach(displayableField -> {
            final String column = this.ledgerColumnMapping.get(displayableField.getName());
            if (column != null) {
                columns.add(column);
            }
        });

        query.append(columns.stream().collect(Collectors.joining(", ")))
                .append(" FROM ")
                .append("thoth_ledgers ledger ");

        final List<QueryParameter> queryParameters = reportRequest.getQueryParameters();
        if (!queryParameters.isEmpty()) {
            final ArrayList<String> criteria = new ArrayList<>();
            queryParameters.forEach(queryParameter -> {
                if(queryParameter.getValue() != null && !queryParameter.getValue().isEmpty()) {
                    criteria.add(
                            CriteriaBuilder.buildCriteria(this.ledgerColumnMapping.get(queryParameter.getName()), queryParameter)
                    );
                }
            });

            if (!criteria.isEmpty()) {
                query.append(" WHERE ");
                query.append(criteria.stream().collect(Collectors.joining(" AND ")));
            }

        }
        query.append(" ORDER BY ledger.identifier");

        return query.toString();
    }

    private String buildSubLedgerQuery(final ReportRequest reportRequest, final String ledgerIdentifier) {
        final List<DisplayableField> displayableFields = reportRequest.getDisplayableFields();
        final ArrayList<String> columns = new ArrayList<>();
        displayableFields.forEach(displayableField -> {
            final String column = this.ledgerColumnMapping.get(displayableField.getName());
            if (column != null) {
                columns.add(column);
            }
        });

        return "SELECT " + columns.stream().collect(Collectors.joining(", ")) + " " +
                "FROM thoth_ledgers ledger " +
                "WHERE ledger.parent_ledger_id ='" + ledgerIdentifier + "' ";
    }


    private String buildAccountQuery(final ReportRequest reportRequest, final String parentLedgerIdentifier) {
        final List<DisplayableField> displayableFields = reportRequest.getDisplayableFields();
        final ArrayList<String> columns = new ArrayList<>();
        displayableFields.forEach(displayableField -> {
            final String column = this.accountColumnMapping.get(displayableField.getName());
            if (column != null) {
                columns.add(column);
            }
        });

        if (!columns.isEmpty()) {
            return "SELECT " + columns.stream().collect(Collectors.joining(", ")) + " " +
                    "FROM thoth_accounts acc " +
                    "LEFT JOIN thoth_ledgers ledger on acc.ledger_id = ledger.id " +
                    "WHERE ledger.id ='" + parentLedgerIdentifier + "' ";
        }
        return null;
    }

}