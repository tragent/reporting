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

@Report(category = "Organization", identifier = "Employee")
public class EmployeeListReportSpecification implements ReportSpecification {

    private static final String USERNAME = "Username";
    private static final String FIRST_NAME = "First Name";
    private static final String MIDDLE_NAME = "Middle Name";
    private static final String LAST_NAME = "Last Name";
    private static final String CREATED_BY = "Created By";

    private static final String OFFICE = "Office Id";
    private static final String OFFICE_NAME = "Office Name";

    private final Logger logger;

    private final EntityManager entityManager;

    private final HashMap<String, String> employeeColumnMapping = new HashMap<>();
    private final HashMap<String, String> officeColumnMapping = new HashMap<>();
    private final HashMap<String, String> allColumnMapping = new HashMap<>();


    @Autowired
    public EmployeeListReportSpecification(@Qualifier(ServiceConstants.LOGGER_NAME) final Logger logger,
                                           final EntityManager entityManager) {
        super();
        this.logger = logger;
        this.entityManager = entityManager;
        this.initializeMapping();
    }

    @Override
    public ReportDefinition getReportDefinition() {
        final ReportDefinition reportDefinition = new ReportDefinition();
        reportDefinition.setIdentifier("Employee");
        reportDefinition.setName("Employee Listing");
        reportDefinition.setDescription("List of all employees.");
        reportDefinition.setQueryParameters(this.buildQueryParameters());
        reportDefinition.setDisplayableFields(this.buildDisplayableFields());
        return reportDefinition;
    }

    @Override
    public ReportPage generateReport(final ReportRequest reportRequest, final int pageIndex, final int size) {
        final ReportDefinition reportDefinition = this.getReportDefinition();
        this.logger.info("Generating report {0}.", reportDefinition.getIdentifier());

        final ReportPage reportPage = new ReportPage();
        reportPage.setName(reportDefinition.getName());
        reportPage.setDescription(reportDefinition.getDescription());
        reportPage.setHeader(this.createHeader(reportRequest.getDisplayableFields()));

        final Query customerQuery = this.entityManager.createNativeQuery(this.buildEmployeeQuery(reportRequest, pageIndex, size));
        final List<?> customerResultList =  customerQuery.getResultList();
        reportPage.setRows(this.buildRows(reportRequest, customerResultList));

        reportPage.setHasMore(
                !this.entityManager.createNativeQuery(this.buildEmployeeQuery(reportRequest, pageIndex + 1, size))
                        .getResultList().isEmpty()
        );

        reportPage.setGeneratedBy(UserContextHolder.checkedGetUser());
        reportPage.setGeneratedOn(DateConverter.toIsoString(LocalDateTime.now(Clock.systemUTC())));
        return reportPage;
    }

    @Override
    public void validate(final ReportRequest reportRequest) throws IllegalArgumentException {
        final ArrayList<String> unknownFields =  new ArrayList<>();
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

    private void initializeMapping() {
        this.employeeColumnMapping.put(USERNAME, "he.identifier");
        this.employeeColumnMapping.put(FIRST_NAME, "he.given_name");
        this.employeeColumnMapping.put(MIDDLE_NAME, "he.middle_name");
        this.employeeColumnMapping.put(LAST_NAME, "he.surname");
        this.employeeColumnMapping.put(CREATED_BY, "he.created_by");
        this.employeeColumnMapping.put(OFFICE, "he.assigned_office_id");

        this.officeColumnMapping.put(OFFICE_NAME, "ho.a_name");

        this.allColumnMapping.putAll(employeeColumnMapping);
        this.allColumnMapping.putAll(officeColumnMapping);
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


    private List<Row> buildRows(final ReportRequest reportRequest, final List<?> employeeResultList) {
        final ArrayList<Row> rows = new ArrayList<>();

        employeeResultList.forEach(result -> {
            final Row row = new Row();
            row.setValues(new ArrayList<>());

            final String officeIdentifier;

            final Object[] resultValues = (Object[]) result;

            officeIdentifier = resultValues[0].toString();

                for (final Object resultValue : resultValues) {
                    final Value value = new Value();
                    if (resultValue != null) {
                        value.setValues(new String[]{resultValue.toString()});
                    } else {
                        value.setValues(new String[]{});
                    }

                    row.getValues().add(value);
                }

            final String officeQueryString = this.buildOfficeQuery(reportRequest, officeIdentifier);
            if (officeQueryString != null) {
                final Query officeQuery = this.entityManager.createNativeQuery(officeQueryString);
                final List<?> resultList = officeQuery.getResultList();
                final Value officeValue = new Value();
                officeValue.setValues(new String[]{resultList.get(0).toString() == null ? " " : resultList.get(0).toString()});
                row.getValues().add(officeValue);
            }

            rows.add(row);
        });

        return rows;
    }

    private List<QueryParameter> buildQueryParameters() {
        return Arrays.asList(
                //QueryParameterBuilder.create(DATE_RANGE, Type.DATE).operator(QueryParameter.Operator.BETWEEN).build(),
                //QueryParameterBuilder.create(STATE, Type.TEXT).operator(QueryParameter.Operator.IN).build()
        );
    }

    private List<DisplayableField> buildDisplayableFields() {
        return Arrays.asList(
                DisplayableFieldBuilder.create(OFFICE, Type.TEXT).mandatory().build(),
                DisplayableFieldBuilder.create(USERNAME, Type.TEXT).mandatory().build(),
                DisplayableFieldBuilder.create(FIRST_NAME, Type.TEXT).mandatory().build(),
                DisplayableFieldBuilder.create(MIDDLE_NAME, Type.TEXT).build(),
                DisplayableFieldBuilder.create(LAST_NAME, Type.TEXT).mandatory().build(),
                DisplayableFieldBuilder.create(CREATED_BY, Type.TEXT).mandatory().build(),
                DisplayableFieldBuilder.create(OFFICE_NAME, Type.TEXT).mandatory().build()
        );
    }

    private String buildEmployeeQuery(final ReportRequest reportRequest, int pageIndex, int size) {
        final StringBuilder query = new StringBuilder("SELECT ");

        final List<DisplayableField> displayableFields = reportRequest.getDisplayableFields();
        final ArrayList<String> columns = new ArrayList<>();
        displayableFields.forEach(displayableField -> {
            final String column = this.employeeColumnMapping.get(displayableField.getName());
            if (column != null) {
                columns.add(column);
            }
        });

        query.append(columns.stream().collect(Collectors.joining(", ")))
                .append(" FROM ")
                .append("horus_employees he ");

        final List<QueryParameter> queryParameters = reportRequest.getQueryParameters();
        if (!queryParameters.isEmpty()) {
            final ArrayList<String> criteria = new ArrayList<>();
            queryParameters.forEach(queryParameter -> {
                if(queryParameter.getValue() != null && !queryParameter.getValue().isEmpty()) {
                    criteria.add(
                            CriteriaBuilder.buildCriteria(this.employeeColumnMapping.get(queryParameter.getName()), queryParameter)
                    );
                }
            });

            if (!criteria.isEmpty()) {
                query.append(" WHERE ");
                query.append(criteria.stream().collect(Collectors.joining(" AND ")));
            }

        }
        query.append(" ORDER BY he.identifier");

        query.append(" LIMIT ");
        query.append(size);
        if (pageIndex > 0) {
            query.append(" OFFSET ");
            query.append(size * pageIndex);
        }

        return query.toString();
    }

    private String buildOfficeQuery(final ReportRequest reportRequest, final String officeIdentifier) {
        final List<DisplayableField> displayableFields = reportRequest.getDisplayableFields();
        final ArrayList<String> columns = new ArrayList<>();
        displayableFields.forEach(displayableField -> {
            final String column = this.officeColumnMapping.get(displayableField.getName());
            if (column != null) {
                columns.add(column);
            }
        });
        if (!columns.isEmpty()) {
        return "SELECT DISTINCT " + columns.stream().collect(Collectors.joining(", ")) + " " +
                "FROM horus_offices ho " +
                "LEFT JOIN horus_employees he on ho.id = he.assigned_office_id " +
                "WHERE he.assigned_office_id ='" + officeIdentifier + "' ";
        }
        return null;
    }
}
