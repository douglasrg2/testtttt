using Educbank.Core.Actions;
using Educbank.Core.BankIntegration;
using Educbank.Core.BankIntegration.Remessa.Santander.Cnab240;
using Educbank.Core.BankIntegration.Remessa.Santander.Cnab400;
using Educbank.Core.CalculatorStrategies;
using Educbank.Core.GatewayPlans;
using Educbank.Core.Ibge;
using Educbank.Core.PaymentGateway;
using Educbank.Core.Schools;
using Educbank.Core.Settings;
using Educbank.Core.Utils;
using Educbank.Core.ValueObjects;
using Educbank.Extensions;
using Educbank.Timing;
using Educbank.ValueObjetcs;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Configuration;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Educbank.Core.GatewayAccounts;
using Volo.Abp;
using Volo.Abp.Caching;
using Volo.Abp.Data;
using Volo.Abp.Domain.Repositories;
using Volo.Abp.MultiTenancy;
using Volo.Abp.ObjectMapping;
using Volo.Abp.Settings;
using Volo.Abp.TenantManagement;
using Action = Educbank.Core.Actions.Action;
using Enrollment = Educbank.Core.Enrollments.Enrollment;
using PlannedInstallment = Educbank.Core.PlannedInstallments.PlannedInstallment;
using Educbank.Logger;

namespace Educbank.Core.Invoices;

[SuppressMessage("ReSharper", "ClassNeverInstantiated.Global")]
public class InvoiceManager : EducbankDomainService
{
    private readonly IInvoiceRepository _invoiceRepository;
    private readonly IDistributedCache<InvoiceCacheItem, Guid> _invoiceCache;
    private readonly ISettingProvider _settingProvider;
    private readonly SchoolManager _schoolManager;
    private readonly IConfiguration _configuration;
    private readonly TenantStore _tenantStore;
    private readonly PaymentGatewayFactory _paymentGatewayFactory;
    private readonly EbpayCalculatorStrategyFactory _ebpayCalculatorStrategy;
    private readonly IRepository<BankIntegrationRemessaFile, Guid> _bankIntegrationRemessaFileRepository;
    private readonly IInflationTaxRepository _inflationTaxRepository;
    private readonly IRepository<GatewayPlan, Guid> _gatewayPlanRepository;
    private readonly IDataFilter<IMultiTenant> _multiTenantDataFilter;
    private readonly IGatewayAccountRepository _gatewayAccountRepository;
    private readonly EducbankLog _educbankLog;
    private readonly IObjectMapper<EducbankDomainModule> _objectMapper;
    private readonly IRepository<Enrollment, Guid> _enrollmentRepository;

    private Dictionary<Guid, string> MapTenantNames { get; set; } = new();

    public InvoiceManager(
        IInvoiceRepository invoiceRepository,
        ISettingProvider settingProvider,
        SchoolManager schoolManager,
        IConfiguration configuration,
        TenantStore tenantStore, PaymentGatewayFactory paymentGatewayFactory,
        EbpayCalculatorStrategyFactory ebpayCalculatorStrategy,
        IRepository<BankIntegrationRemessaFile, Guid> bankIntegrationRemessaFileRepository,
        IInflationTaxRepository inflationTaxRepository,
        IRepository<GatewayPlan, Guid> gatewayPlanRepository,
        IDataFilter<IMultiTenant> multiTenantDataFilter,
        EducbankLog educbankLog,
        IDistributedCache<InvoiceCacheItem, Guid> invoiceCache,
        IObjectMapper<EducbankDomainModule> objectMapper,
        IGatewayAccountRepository gatewayAccountRepository,
        IRepository<Enrollment, Guid> enrollmentRepository)
    {
        _invoiceRepository = invoiceRepository;
        _settingProvider = settingProvider;
        _schoolManager = schoolManager;
        _configuration = configuration;
        _tenantStore = tenantStore;
        _paymentGatewayFactory = paymentGatewayFactory;
        _ebpayCalculatorStrategy = ebpayCalculatorStrategy;
        _bankIntegrationRemessaFileRepository = bankIntegrationRemessaFileRepository;
        _gatewayPlanRepository = gatewayPlanRepository;
        _multiTenantDataFilter = multiTenantDataFilter;
        _gatewayAccountRepository = gatewayAccountRepository;
        _inflationTaxRepository = inflationTaxRepository;
        _invoiceCache = invoiceCache;
        _objectMapper = objectMapper;
        _educbankLog = educbankLog;
        _enrollmentRepository = enrollmentRepository;
    }

    public async Task<Invoice> Create(string externalId, DateTime dueDate, Payer payer,
        List<PlannedInstallment> plannedInstallments, List<EarlyPaymentDiscount> earlyPaymentDiscounts,
        PaymentFine paymentFine, List<CustomVariables> customVariables, string referenceDate, string companyCnpj,
        string code = null)
    {
        if (!await ValidateGatewayProduct(plannedInstallments))
            throw new BusinessException(ExceptionConsts.Invoice.CannotCreateInvoiceWithGatewayPermissionDisable);

        await ValidateEbpayPlan(plannedInstallments);

        await ValidatePaymentFine(paymentFine);

        var companyDocument = new Document(companyCnpj, DocumentType.Cnpj);

        var school = await TryGetSchoolByCnpj(companyDocument);

        await CheckExternalId(externalId);

        var forcePaymentFine = await _settingProvider.GetAsync<bool>(EducbankSettings.ForcePaymentFineSettings);
        if (forcePaymentFine)
            paymentFine = await GetPaymentFineFromSettings();

        referenceDate = CheckReferenceDate(dueDate, referenceDate);
        code ??= await GetUniqueIdentifierCode(school, referenceDate);
        var guid = GuidGenerator.Create();
        var url = await GetInvoiceUrl(guid);

        var invoice = new Invoice(guid, CurrentTenant.Id, payer, dueDate, externalId, referenceDate,
            companyDocument, school.Name, code, plannedInstallments, earlyPaymentDiscounts, school.Id, null,
            paymentFine, url);

        if (customVariables != null)
        {
            foreach (var customVariable in customVariables)
            {
                invoice.AddCustomVariable(customVariable.Name, customVariable.Value);
            }
        }

        invoice.AddInvoiceLog(new InvoiceLog(null, "Invoice created"));
        invoice.AddEvent(InvoiceEventType.Created, GuidGenerator.Create());
        return invoice;
    }

    public async Task<Invoice> Import(string externalId, DateTime dueDate,
        Payer payer, List<PlannedInstallment> plannedInstallments,
        List<EarlyPaymentDiscount> earlyPaymentDiscounts,
        List<PaymentGatewayInvoice> paymentGatewayInvoices, PaymentFine paymentFine,
        List<CustomVariables> customVariables, string referenceDate, string companyCnpj, string code = null)
    {
        if (plannedInstallments == null || !plannedInstallments.Any())
            throw new BusinessException(ExceptionConsts.Invoice.MustHaveAtLeastOnePlannedInstallment);

        var companyDocument = new Document(companyCnpj, DocumentType.Cnpj);

        var school = await TryGetSchoolByCnpj(companyDocument);

        await CheckExternalId(externalId);

        var forcePaymentFine = await _settingProvider.GetAsync<bool>(EducbankSettings.ForcePaymentFineSettings);
        if (forcePaymentFine)
            paymentFine = await GetPaymentFineFromSettings();

        referenceDate = CheckReferenceDate(dueDate, referenceDate);
        code ??= await GetUniqueIdentifierCode(school, referenceDate);
        var guid = GuidGenerator.Create();
        var url = await GetInvoiceUrl(guid);

        var invoice = new Invoice(guid, CurrentTenant.Id, payer, dueDate,
            externalId, referenceDate, companyDocument, school.Name, code, plannedInstallments,
            earlyPaymentDiscounts, school.Id,
            paymentGatewayInvoices, paymentFine, url);

        if (customVariables != null)
        {
            foreach (var customVariable in customVariables)
            {
                invoice.AddCustomVariable(customVariable.Name, customVariable.Value);
            }
        }

        invoice.AddInvoiceLog(new InvoiceLog(null, "Invoice created"));

        return invoice;
    }

    public async Task<List<Invoice>> CreateMany(DateTime dueDate, Payer payer,
        List<PlannedInstallment> plannedInstallments, List<EarlyPaymentDiscount> earlyPaymentDiscounts,
        List<CustomVariables> customVariables, string companyCnpj)
    {
        earlyPaymentDiscounts ??= new List<EarlyPaymentDiscount>();
        var groupedInstallment = plannedInstallments.GroupBy(x => x.InstallmentNumber);

        var paymentFine = await GetPaymentFineFromSettings();

        var invoiceList = new List<Invoice>();

        var totalItemCents = plannedInstallments.Sum(x => x.ValueCents);

        foreach (var group in groupedInstallment)
        {
            if (group.Key == null) continue;
            var plannedInstallmentDueDate = dueDate.Date.AddMonths(group.Key.Value - 1);

            var earlyPaymentDiscountsInvoice = earlyPaymentDiscounts
                .Select(item =>
                    new EarlyPaymentDiscount(item.Days, item.Percent, item.ValueCents, plannedInstallmentDueDate,
                        totalItemCents))
                .ToList();

            var invoice = await Create(null, plannedInstallmentDueDate, payer, @group.ToList(),
                earlyPaymentDiscountsInvoice, paymentFine, customVariables, null, companyCnpj);

            invoiceList.Add(invoice);
        }

        return invoiceList;
    }

    public async Task<PaymentGatewayInvoice> Close(Invoice invoice, CloseInvoiceCommand command)
    {
        var eventId = GuidGenerator.Create();
        invoice.AddEvent(InvoiceEventType.Generated, eventId);
        var paymentGatewayInvoice = invoice.Close(GuidGenerator.Create(), command.GatewayAccount.PaymentGatewayType, eventId,
            command.GatewayAccount.Id, command.CreateAsync, command.DueDate, command.Charges);

        var paymentGateway = _paymentGatewayFactory.GetPaymentGateway(command.GatewayAccount.PaymentGatewayType);

        if (!command.CreateAsync)
        {
            await paymentGateway.CreateInvoice(paymentGatewayInvoice, invoice);

            if (paymentGatewayInvoice.Errors is { Count: > 0 })
            {
                throw new BusinessException(ExceptionConsts.Invoice.ErrorOnCreationWithData)
                    .WithData("errorList", string.Join("; ", paymentGatewayInvoice.Errors));
            }
        }

        var zeroDefaultStrategy = _ebpayCalculatorStrategy.GetStrategy(CalculatorStrategyEnum.ZeroDefault);
        await zeroDefaultStrategy.AddInvoiceCreatedTransactions(invoice, EbClock.Now, eventId);
        invoice.AddInvoiceLog(new InvoiceLog(command));
        var enrollmentIds = invoice.PlannedInstallments.Select(x=> x.EnrollmentId.Value);
        var enrollments = await _enrollmentRepository.GetListAsync(x=> enrollmentIds.Contains(x.Id));
        var guardians = enrollments.SelectMany(x=> x.Guardians);
        var guardianNames = string.Join(',', guardians.Select(x=> x.Name));
        invoice.AddClosedEvent(eventId, paymentGatewayInvoice.Id, command.CreateAsync, guardianNames, InvoiceEventType.Generated.ToString());
        return paymentGatewayInvoice;
    }

    public async Task Update(Invoice invoice, UpdateInvoiceCommand command)
    {
        if (!await ValidateGatewayProduct(command.PlannedInstallments))
            throw new BusinessException(ExceptionConsts.Invoice.CannotUpdateInvoiceWithGatewayPermissionDisable);

        var isClosed = invoice.IsClosed;

        await ValidateEbpayPlan(command.PlannedInstallments);

        await ValidatePaymentFine(command.PaymentFine);

        var eventId = GuidGenerator.Create();
        invoice.AddEvent(InvoiceEventType.Updated, eventId);

        invoice.Update(command.DueDate, command.PaymentFine, command.Payer, command.PlannedInstallments,
            command.EarlyPaymentDiscounts, command.Charges, eventId);

        if (isClosed)
        {
            var actualPaymentGatewayFromInvoice = invoice.PaymentGatewayInvoice;
            var paymentGatewayType = actualPaymentGatewayFromInvoice.PaymentGatewayType;
            var paymentGatewayFromInvoice = _paymentGatewayFactory.GetPaymentGateway(paymentGatewayType);

            await CreateNewPaymentGatewayToInvoice(invoice, command);

            await paymentGatewayFromInvoice.CancelInvoice(invoice, actualPaymentGatewayFromInvoice);
            actualPaymentGatewayFromInvoice.Cancel(InvoiceCancelationReason.InvoiceUpdated);
        }

        invoice.AddInvoiceLog(new InvoiceLog(command));
        var zeroDefaultStrategy = _ebpayCalculatorStrategy.GetStrategy(CalculatorStrategyEnum.ZeroDefault);
        await zeroDefaultStrategy.AddInvoiceUpdatedTransactions(invoice, EbClock.Now, eventId);
        var enrollmentIds = invoice.PlannedInstallments.Select(x=> x.EnrollmentId.Value);
        var enrollments = await _enrollmentRepository.GetListAsync(x=> enrollmentIds.Contains(x.Id));
        var guardians = enrollments.SelectMany(x=> x.Guardians);
        var guardianNames = string.Join(',', guardians.Select(x=> x.Name));
        invoice.AddUpdatedEvent(eventId, guardianNames, InvoiceEventType.Updated.ToString());
    }

    private async Task CreateNewPaymentGatewayToInvoice(Invoice invoice, UpdateInvoiceCommand command)
    {
        var gatewayAccount =
            await _gatewayAccountRepository.GetDefaultGatewayAccountCacheItem(invoice.TenantId, invoice.EbpayPlan);
        var actualGatewayService = _paymentGatewayFactory.GetPaymentGateway(gatewayAccount.PaymentGatewayType);

        var paymentGatewayInvoice = new PaymentGatewayInvoice(GuidGenerator.Create(), gatewayAccount.PaymentGatewayType,
            gatewayAccount.Id, invoice.Payer, invoice.DueDate, invoice.ReferenceDate, invoice.PlannedInstallments,
            command.Charges, invoice.PaymentFine, invoice.EarlyPaymentDiscounts);

        invoice.PaymentGatewayInvoices.AddFirst(paymentGatewayInvoice);
        await actualGatewayService.CreateInvoice(paymentGatewayInvoice, invoice);

        if (paymentGatewayInvoice.Errors.Any())
        {
            throw new BusinessException(ExceptionConsts.Invoice.ErrorOnCreationWithData)
                .WithData("errorList", string.Join(" | ", paymentGatewayInvoice.Errors.ToList()));
        }
    }

    public async Task Cancel(Invoice invoice, CancelInvoiceCommand command)
    {
        await ValidateFineAndCharges(invoice);
        var eventId = GuidGenerator.Create();
        invoice.AddEvent(InvoiceEventType.Canceled, eventId);

        var gatewayAccountId = invoice.PaymentGatewayInvoice?.GatewayAccountId;
        if (gatewayAccountId == null)
        {
            var gatewayAccount =
                await _gatewayAccountRepository.GetDefaultGatewayAccountByPaymentTypeCacheItem(PaymentGatewayType
                    .Local);
            gatewayAccountId = gatewayAccount.Id;
        }

        invoice.Cancel(command.Reason, gatewayAccountId.Value, eventId);

        if (command.Reason != InvoiceCancelationReason.PaidAtSchool &&
            command.Reason != InvoiceCancelationReason.ExpiredPaidAtSchool &&
            command.Reason != InvoiceCancelationReason.PaymentGatewayUpdated)
        {
            _educbankLog.AddProperty("PaymentGatewayToCancel", invoice.PaymentGatewayInvoice).Log("CancelMethodCancelling").Information("Iniciando cancelamento no gateway");
            var paymentGateway = _paymentGatewayFactory.GetPaymentGateway(invoice.PaymentGatewayInvoice.PaymentGatewayType);
            await paymentGateway.CancelInvoice(invoice, invoice.PaymentGatewayInvoice);

            var zeroDefaultStrategy = _ebpayCalculatorStrategy.GetStrategy(CalculatorStrategyEnum.ZeroDefault);
            await zeroDefaultStrategy.AddInvoiceCanceledTransactions(invoice, EbClock.Now, eventId);
        }

        invoice.AddInvoiceLog(new InvoiceLog(command));
        if(!invoice.ValidTransactions.Any(x=> x.TransactionType == TransactionType.Transfer || x.TransactionType == TransactionType.Retention))
        {
            invoice.LiquidateByCancellation(EbClock.Now, invoice.ValueToCalculateTransfer);
        }
        var enrollmentIds = invoice.PlannedInstallments.Select(x=> x.EnrollmentId.Value);
        var enrollments = await _enrollmentRepository.GetListAsync(x=> enrollmentIds.Contains(x.Id));
        var guardians = enrollments.SelectMany(x=> x.Guardians);
        var guardianNames = string.Join(',', guardians.Select(x=> x.Name));
        invoice.AddCanceledEvent(eventId, guardianNames, InvoiceEventType.Canceled.ToString());
    }

    private async Task ReversePaymentsAtSchool(Invoice invoice, CancelInvoiceCommand command, Guid eventId)
    {
        await ValidateFineAndCharges(invoice);
        invoice.CancelPaymentsAtSchool(command.Reason);
        invoice.AddInvoiceLog(new InvoiceLog(command));
        var zeroDefaultStrategy = _ebpayCalculatorStrategy.GetStrategy(CalculatorStrategyEnum.ZeroDefault);
        await zeroDefaultStrategy.AddInvoiceReversePaymentAtSchoolTransactions(invoice, eventId);
    }

    public async Task RollbackCancelPaymentsAtSchool(Invoice invoice, RollbackInvoiceSchoolPaymentCommand command, Guid eventId)
    {
        invoice.RollbackCancelPaymentsAtSchool();
        invoice.AddInvoiceLog(new InvoiceLog(command.RawEvent, command.Description, false));
        var zeroDefaultStrategy = _ebpayCalculatorStrategy.GetStrategy(invoice.EbpayPlan);
        await zeroDefaultStrategy.AddInvoiceReversePaymentAtSchoolTransactions(invoice, eventId);
    }

    private async Task ReverseCancellation(Invoice invoice, Guid eventId)
    {
        await ValidateFineAndCharges(invoice);
        var zeroDefaultStrategy = _ebpayCalculatorStrategy.GetStrategy(CalculatorStrategyEnum.ZeroDefault);
        await zeroDefaultStrategy.AddInvoiceReverseCancellationTransactions(invoice, eventId);
    }

    public async Task Expire(Invoice invoice, ExpireInvoiceCommand command)
    {
        var status = new List<PaymentGatewayInvoiceState>(new[]
        {
            PaymentGatewayInvoiceState.Paid,
            PaymentGatewayInvoiceState.Canceled,
            PaymentGatewayInvoiceState.Expired
        });
        if (status.Contains(invoice.State))
            return;

        invoice.Expire(command.PaymentGatewayInvoiceId);
        invoice.AddInvoiceLog(new InvoiceLog(command));

        await Task.CompletedTask;
    }

    public async Task PayAtConciliation(Invoice invoice, PayConciliationCommand command)
    {
        await ValidateFineAndCharges(invoice);

        var eventId = GuidGenerator.Create();
        invoice.AddEvent(InvoiceEventType.Paid, eventId);

        if (invoice.State == PaymentGatewayInvoiceState.Canceled)
        {
            await ReverseCancellation(invoice, eventId);
        }
        else if (invoice.State == PaymentGatewayInvoiceState.Paid &&
                 invoice.PaidMethod == InvoicePaymentMethod.School)
        {
            await ReversePaymentsAtSchool(invoice, new CancelInvoiceCommand
            {
                Description = "Reverse payment at school by pay at conciliation",
                Reason = InvoiceCancelationReason.PaymentGatewayUpdated
            }, eventId);
        }

        var paymentGatewayInvoice = TryGetPaymentGatewayInvoice(invoice, command.PaymentGatewayInvoiceId);

        command.GatewayPaymentDate ??= command.PaymentDate;

        invoice.PayAtConciliation(paymentGatewayInvoice, command.PaymentDate, command.TotalPaidCents, eventId,
            commissionCents: command.CommissionCents,
            taxesPaidCents: command.TaxesPaidCents, paymentMethod: command.PaymentMethod,
            gatewayPaymentDate: command.GatewayPaymentDate, effectiveDiscountCents: command.EffectiveDiscountCents,
            effectiveTotalFineCents: command.EffectiveTotalFineCents);

        invoice.AddInvoiceLog(new InvoiceLog(command));
        var zeroDefaultStrategy = _ebpayCalculatorStrategy.GetStrategy(CalculatorStrategyEnum.ZeroDefault);
        await zeroDefaultStrategy.AddInvoicePaymentTransactions(invoice, command.PaymentDate, eventId);
        var enrollmentIds = invoice.PlannedInstallments.Select(x=> x.EnrollmentId.Value);
        var enrollments = await _enrollmentRepository.GetListAsync(x=> enrollmentIds.Contains(x.Id));
        var guardians = enrollments.SelectMany(x=> x.Guardians);
        var guardianNames = string.Join(',', guardians.Select(x=> x.Name));
        invoice.AddPaidEvent(eventId, guardianNames, InvoiceEventType.Paid.ToString());
    }

    public async Task Pay(Invoice invoice, PayInvoiceCommand command)
    {
        await ValidateFineAndCharges(invoice);
        var eventId = GuidGenerator.Create();
        invoice.AddEvent(InvoiceEventType.Paid, eventId);

        if (invoice.State == PaymentGatewayInvoiceState.Canceled)
        {
            await ReverseCancellation(invoice, eventId);
        }
        else if (invoice.State == PaymentGatewayInvoiceState.Paid &&
                 invoice.PaidMethod == InvoicePaymentMethod.School)
        {
            await ReversePaymentsAtSchool(invoice, new CancelInvoiceCommand
            {
                Description = "Reverse payment at school by pay",
                Reason = InvoiceCancelationReason.PaymentGatewayUpdated
            }, eventId);
        }

        var paymentGatewayInvoice = TryGetPaymentGatewayInvoice(invoice, command.PaymentGatewayInvoiceId);

        command.GatewayPaymentDate ??= command.PaymentDate;

        if (command.TotalPaidCents != invoice.TotalCents)
        {
            paymentGatewayInvoice
                .SetPaymentDifference(invoice.TotalCents, command.TotalPaidCents);
        }

        invoice.Pay(paymentGatewayInvoice, command.PaymentDate, command.TotalPaidCents, eventId,
            commissionCents: command.CommissionCents,
            taxesPaidCents: command.TaxesPaidCents, paymentMethod: command.PaymentMethod,
            gatewayPaymentDate: command.GatewayPaymentDate, effectiveDiscountCents: command.EffectiveDiscountCents,
            effectiveTotalFineCents: command.EffectiveTotalFineCents);

        invoice.AddInvoiceLog(new InvoiceLog(command));
        var zeroDefaultStrategy = _ebpayCalculatorStrategy.GetStrategy(CalculatorStrategyEnum.ZeroDefault);
        await zeroDefaultStrategy.AddInvoicePaymentTransactions(invoice, command.PaymentDate, eventId);
        var enrollmentIds = invoice.PlannedInstallments.Select(x=> x.EnrollmentId.Value);
        var enrollments = await _enrollmentRepository.GetListAsync(x=> enrollmentIds.Contains(x.Id));
        var guardians = enrollments.SelectMany(x=> x.Guardians);
        var guardianNames = string.Join(',', guardians.Select(x=> x.Name));
        invoice.AddPaidEvent(eventId, guardianNames, InvoiceEventType.Paid.ToString());
        if(command.TotalPaidCents >= invoice.GetNetValueByDate(command.PaymentDate))
        {
            if(invoice.Liquidation?.Date != null)
            {
                invoice.AddLiquidationDuplicatedEvent(command.PaymentDate);
            }
            else
            {
                invoice.LiquidateByPayment(command.PaymentDate,command.TotalPaidCents);
            }
        }
    }

    public async Task<Invoice> PayAtSchool(Invoice invoice, PayAtSchoolCommand command)
    {
        await ValidateFineAndCharges(invoice);

        var eventId = GuidGenerator.Create();
        invoice.AddEvent(InvoiceEventType.PaidAtSchool, eventId);

        if (invoice.State == PaymentGatewayInvoiceState.Canceled)
        {
            await ReverseCancellation(invoice, eventId);
        }

        var paidDate = command.PaidDate ?? EbClock.GetBrasiliaToday();
        var totalPaid = command.TotalPaidCents ?? invoice.TotalCents;
        var paidDiff = totalPaid - invoice.TotalItemsCents;
        var effectiveTotalFineCents = paidDiff > 0 ? paidDiff : 0;
        var effectiveDiscountCents = paidDiff < 0 ? paidDiff * -1 : 0;

        if(invoice.PaymentGatewayInvoice != null)
        {
            var paymentGateway = _paymentGatewayFactory.GetPaymentGateway(invoice.PaymentGatewayInvoice.PaymentGatewayType);
            _educbankLog.AddProperty("PaymentGatewayToCancel", invoice.PaymentGatewayInvoice).Log("PayAtSchollMethodCancelling").Information("Iniciando cancelamento no gateway");
            await paymentGateway.CancelInvoice(invoice, invoice.PaymentGatewayInvoice);
        }

        var gatewayAccount =
            await _gatewayAccountRepository
                .GetDefaultGatewayAccountByPaymentTypeCacheItem(PaymentGatewayType.Local);
        var oldPaymentGatewayInvoice = invoice.PayAtSchool(paidDate, gatewayAccount.Id, totalPaid, eventId, effectiveDiscountCents, effectiveTotalFineCents);

        invoice.AddInvoiceLog(new InvoiceLog(command));
        var zeroDefaultStrategy = _ebpayCalculatorStrategy.GetStrategy(CalculatorStrategyEnum.ZeroDefault);
        await zeroDefaultStrategy.AddInvoicePaymentTransactions(invoice, paidDate, eventId);
        var enrollmentIds = invoice.PlannedInstallments.Select(x=> x.EnrollmentId.Value);
        var enrollments = await _enrollmentRepository.GetListAsync(x=> enrollmentIds.Contains(x.Id));
        var guardians = enrollments.SelectMany(x=> x.Guardians);
        var guardianNames = string.Join(',', guardians.Select(x=> x.Name));
        invoice.AddPaidAtSchoolEvent(eventId, oldPaymentGatewayInvoice.Id, guardianNames, InvoiceEventType.PaidAtSchool.ToString());
        return invoice;
    }

    public async Task<Invoice> PayInternal(Invoice invoice, InvoicePaymentMethod invoicePaymentMethod)
    {
        var eventId = GuidGenerator.Create();
        invoice.AddEvent(InvoiceEventType.Paid, eventId);

        if (invoice.State == PaymentGatewayInvoiceState.Canceled)
            await ReverseCancellation(invoice, eventId);

        var paidDate = EbClock.GetBrasiliaToday();
        var totalPaid = invoice.TotalCents;
        var paidDiff = totalPaid - invoice.TotalItemsCents;
        var effectiveTotalFineCents = paidDiff > 0 ? paidDiff : 0;
        var effectiveDiscountCents = paidDiff < 0 ? paidDiff * -1 : 0;

        await Pay(
            invoice,
            new PayInvoiceCommand(
                invoice.PaymentGatewayInvoice.Id,
                paidDate,
                null,
                invoice.TotalFineCents,
                totalPaid,
                invoicePaymentMethod,
                paidDate,
                effectiveDiscountCents,
                effectiveTotalFineCents
            ));

        return invoice;
    }

    public async Task PayByCreditCard(Invoice invoice, string creditCardToken, byte numberOfInstallments,
        CardBrand brand)
    {
        await ValidateCreditCardPayment(invoice, numberOfInstallments);
        var paymentGatewayManager = _paymentGatewayFactory.GetPaymentGateway(PaymentGatewayType.Zoop);
        var creditCardData =
            await paymentGatewayManager.PayByCreditCard(invoice, creditCardToken, numberOfInstallments, brand);
        var creditCardTaxCents =
            await GetCreditCardChargesCents(invoice, invoice.TotalCents, numberOfInstallments, brand);
        var valueCents = invoice.TotalCents;
        var chargeResponsible = await _settingProvider.GetOrNullAsync(EducbankSettings.ResponsibleForCreditCardTax);
        var parseResult = Enum.TryParse(chargeResponsible, out ResponsibleForCreditCardTaxType responsible);
        if (!parseResult)
            return;
        if (responsible == ResponsibleForCreditCardTaxType.Guardian)
            valueCents += creditCardTaxCents;

        var eventId = GuidGenerator.Create();
        invoice.AddEvent(InvoiceEventType.Paid, eventId);

        var gatewayFeeCents = Convert.ToInt64(creditCardData.Fees * 100);
        invoice.PayByCreditCard(invoice.PaymentGatewayInvoice.GatewayAccountId, valueCents, eventId,
            invoice.TotalDiscountCents, invoice.TotalFineCents, creditCardTaxCents, creditCardData.Id,
            gatewayFeeCents);

        invoice.PaymentGatewayInvoice.CreditCard = creditCardData;
        var zeroDefaultStrategy = _ebpayCalculatorStrategy.GetStrategy(CalculatorStrategyEnum.ZeroDefault);
        await zeroDefaultStrategy.WritePaymentByCreditCardTransactions(invoice, EbClock.Now, creditCardTaxCents, eventId);
        var enrollmentIds = invoice.PlannedInstallments.Select(x=> x.EnrollmentId.Value);
        var enrollments = await _enrollmentRepository.GetListAsync(x=> enrollmentIds.Contains(x.Id));
        var guardians = enrollments.SelectMany(x=> x.Guardians);
        var guardianNames = string.Join(',', guardians.Select(x=> x.Name));
        invoice.AddPaidEvent(eventId, guardianNames, InvoiceEventType.Paid.ToString());
        if(responsible != ResponsibleForCreditCardTaxType.School)
        {
            invoice.LiquidateByCreditCardPayment(valueCents);
        }
    }

    public async Task<Invoice> PayDuplicated(Invoice invoice, PayDuplicatedCommand command)
    {
        await ValidateFineAndCharges(invoice);

        var paidDate = command.PaidDate ?? EbClock.GetBrasiliaToday();
        var duplicatedTotalPaid = command.DuplicatedTotalPaid;

        invoice.AddInvoiceLog(new InvoiceLog(command));
        var eventId = GuidGenerator.Create();
        invoice.AddEvent(InvoiceEventType.DuplicatePayment, eventId);
        var zeroDefaultStrategy = _ebpayCalculatorStrategy.GetStrategy(CalculatorStrategyEnum.ZeroDefault);
        await zeroDefaultStrategy.AddInvoicePaymentDuplicatedTransactions(invoice, paidDate, duplicatedTotalPaid, eventId);
        var enrollmentIds = invoice.PlannedInstallments.Select(x=> x.EnrollmentId.Value);
        var enrollments = await _enrollmentRepository.GetListAsync(x=> enrollmentIds.Contains(x.Id));
        var guardians = enrollments.SelectMany(x=> x.Guardians);
        var guardianNames = string.Join(',', guardians.Select(x=> x.Name));
        invoice.AddDuplicatedPaymentEvent(eventId, guardianNames, InvoiceEventType.DuplicatePayment.ToString());
        return invoice;
    }

    public static void Error(Invoice invoice, ErrorInvoiceCommand command)
    {
        invoice.Error(command.PaymetGatewayInvoiceId, command.Errors, command.RawEvent);
        invoice.AddInvoiceLog(new InvoiceLog(command));
    }

    public static void UpdateAdmin(Invoice invoice, UpdateAdminInvoiceCommand command)
    {
        if (!command.ReferenceDate.IsNullOrEmpty())
            invoice.SetReferenceDate(command.ReferenceDate);

        if (command.Payer != null)
            invoice.SetPayer(command.Payer);

        invoice.AddInvoiceLog(new InvoiceLog(command));
    }

    public async Task<IQueryable<Invoice>> GetInvoiceByDueDate(DateTime? initialDate, DateTime? endDate)
    {
        var invoiceQuery = await _invoiceRepository.GetMongoQueryableAsync();

        if (initialDate.HasValue && endDate.HasValue)
            invoiceQuery = invoiceQuery.Where(x => x.DueDate <= endDate && x.DueDate >= initialDate);

        else if (initialDate.HasValue)
            invoiceQuery = invoiceQuery.Where(x => x.DueDate >= initialDate);

        else if (endDate.HasValue)
            invoiceQuery = invoiceQuery.Where(x => x.DueDate <= endDate);

        return invoiceQuery;
    }

    public async Task<IQueryable<Invoice>> GetInvoiceRescheduledByTransferDate(Guid? schoolTenantId,
        DateTime? initialDate, DateTime? endDate)
    {
        var invoiceQuery = await _invoiceRepository.GetMongoQueryableAsync();

        if (schoolTenantId.HasValue)
            invoiceQuery = invoiceQuery.Where(x => x.TenantId.Equals(schoolTenantId));

        if (initialDate.HasValue && endDate.HasValue)
            invoiceQuery = invoiceQuery.Where(x =>
                x.TransferDateRescheduled != null && x.TransferDateRescheduled <= endDate &&
                x.TransferDateRescheduled >= initialDate);

        else if (initialDate.HasValue)
            invoiceQuery = invoiceQuery.Where(x =>
                x.TransferDateRescheduled != null && x.TransferDateRescheduled >= initialDate);

        else if (endDate.HasValue)
            invoiceQuery = invoiceQuery.Where(x =>
                x.TransferDateRescheduled != null && x.TransferDateRescheduled <= endDate);

        return invoiceQuery;
    }

    public async Task Duplicate(Invoice invoice, UpdatePaymentGatewayInvoiceCommand command)
    {
        const string formatDateValid = "yyyy-MM-dd";
        var dueDateIsValid = command.DueDate.TryParseValidDateTime(out var newDueDate, formatDateValid);
        if (!dueDateIsValid)
        {
            throw new BusinessException(ExceptionConsts.InvalidDateFormat).WithData("format", formatDateValid);
        }

        await ValidateFineAndCharges(invoice);

        var overdueFine = await _settingProvider.GetAsync<decimal>(EducbankSettings.OverdueFine);
        var dailyInterest = await _settingProvider.GetAsync<decimal>(EducbankSettings.DailyInterest);
        var educbankPaymentFine = new PaymentFine(overdueFine, dailyInterest);

        var chargesToNewPaymentGateway =
            educbankPaymentFine.GetPaymentFineCents(invoice.DueDate, invoice.GetTotalItemsCents(), newDueDate);
        var eventId = GuidGenerator.Create();
        invoice.AddEvent(InvoiceEventType.Duplicated, eventId);
        await invoice.Duplicate(newDueDate, chargesToNewPaymentGateway, _paymentGatewayFactory, eventId);
    }

    public async Task<InvoiceCacheItem> GetInvoiceFromCache(Guid id)
    {
        return await _invoiceCache.GetOrAddAsync(
            id, //cache key
            async () => await InvoiceCacheFactory(id),
            () => new DistributedCacheEntryOptions
            {
                AbsoluteExpiration = EbClock.Now.AddHours(6)
            }
        );
    }

    private async Task<InvoiceCacheItem> InvoiceCacheFactory(Guid id)
    {
        var invoice = await _invoiceRepository.FirstOrDefaultAsync(x => x.Id == id);
        if (invoice == null)
            throw new BusinessException(ExceptionConsts.Invoice.NotFound);

        //validade fine and charges
        using (CurrentTenant.Change(invoice.TenantId))
        {
            await ValidateFineAndCharges(invoice);
        }
        //if is zoop, generate pix
        var isZoopExpiredPix = invoice.PaymentGatewayInvoice?.PaymentGatewayType == PaymentGatewayType.Zoop &&
                               invoice.State is PaymentGatewayInvoiceState.Expired or PaymentGatewayInvoiceState.Pending &&
                               (invoice.PaymentGatewayInvoice?.Pix == null ||
                                invoice.PaymentGatewayInvoice is { Pix: { IsExpired: true } });
        if (isZoopExpiredPix)
            invoice =  await GeneratePix(invoice);

        //map
        var invoiceCacheItem = _objectMapper.Map<Invoice, InvoiceCacheItem>(invoice);

        if (invoiceCacheItem.BankSlip != null)
            invoiceCacheItem.BankSlip.Url = invoiceCacheItem.Url;

        var customVariables = invoice.CustomVariables ?? new List<CustomVariables>();
        if (customVariables.Any())
        {
            foreach (var variable in customVariables)
            {
                if (variable.Name == BankIntegrationConsts.RemessaInvoiceCustomVariableName)
                {
                    invoiceCacheItem.RemessaId = Guid.Parse(variable.Value);
                    break;
                }
            }
        }

        if (invoice.PaymentGatewayInvoice == null
            || invoice.PaymentGatewayInvoice.PaymentGatewayType == PaymentGatewayType.Iugu)
        {
            invoiceCacheItem.IntermediaryName = "Iugu Serviços na Internet SA";
            invoiceCacheItem.IntermediaryCnpj = "15.111.975/0001-64";
        }
        else if (invoice.PaymentGatewayInvoice.PaymentGatewayType == PaymentGatewayType.Zoop)
        {
            invoiceCacheItem.IntermediaryName = "ZOOP TECNOLOGIA E MEIOS DE PAGAMENTO S.A";
            invoiceCacheItem.IntermediaryCnpj = "19.468.242/0001-32";
        }
        else if (invoice.PaymentGatewayInvoice.PaymentGatewayType == PaymentGatewayType.Internal)
        {
            invoiceCacheItem.IntermediaryName = "Educbank Internal Gateway";
            invoiceCacheItem.IntermediaryCnpj = "37.315.476/0001-21";
        }

        //set description
        if(invoice.PaymentGatewayInvoice != null)
            invoiceCacheItem.Description = GetInvoiceBankSlipDescription(invoice.PaymentGatewayInvoice);

        //set url and Expiration date for bankslip
        if (invoiceCacheItem.BankSlip != null)
        {
            invoiceCacheItem.BankSlip.Url = invoiceCacheItem.Url;
            invoiceCacheItem.BankSlip.ExpirationDate = invoiceCacheItem.PaymentGatewayDueDate;
        }

        List<string> errors = new List<string>();
        foreach (var error in invoiceCacheItem.Errors.Where(error => !error.IsNullOrEmpty()))
        {
            if (error.Contains(":Error:"))
                errors.Add(error.Split(' ')[0] + " " + ExceptionLocalizer[error.Split(' ')[1]]);
            else
                errors.Add(error);
        }

        invoiceCacheItem.Errors = errors;
        invoiceCacheItem.TenantId = CurrentTenant.Id == null ? invoice.TenantId : null;

        return invoiceCacheItem;
    }

    private static string[] GetInvoiceBankSlipDescription(PaymentGatewayInvoice paymentGatewayInvoice)
    {
        if (paymentGatewayInvoice == null)
            throw new BusinessException(ExceptionConsts.Invoice.NotFound);

        var result = new List<string>();

        if (paymentGatewayInvoice.PaymentFine != null)
        {
            var fineAmount = Convert
                .ToDecimal(paymentGatewayInvoice.PaymentFine
                    .GetOverdueFineCents(paymentGatewayInvoice.TotalItemsCents)) / 100;

            var dailyLatePaymentAmount = Convert.ToDecimal(
                paymentGatewayInvoice.PaymentFine
                    .GetDailyInterestCents(paymentGatewayInvoice.TotalItemsCents)) / 100;

            result.Add(
                $"Após o vencimento cobrar: Multa por atraso de R$ {fineAmount.ToBrazilianString()} e Mora diária de R$ {dailyLatePaymentAmount.ToBrazilianString()}");
        }

        if (paymentGatewayInvoice.HasEarlyPaymentDiscounts())
        {
            foreach (var earlyPaymentDiscount in paymentGatewayInvoice.EarlyPaymentDiscounts)
            {
                var discountDate = paymentGatewayInvoice.DueDate.AddDays(-earlyPaymentDiscount.Days);
                var invoiceValueWithDiscount = Convert.ToDecimal(
                    paymentGatewayInvoice.TotalItemsCents - earlyPaymentDiscount.EffectiveDiscountCents) / 100;

                result.Add(
                    $"Para pagamento até dia {discountDate:dd/MM/yyyy} receber valor de R$ {invoiceValueWithDiscount.ToBrazilianString()}");
            }
        }

        return result.ToArray();
    }

    private async Task<Invoice> GeneratePix(Invoice invoice)
    {
        var invoiceGateway = invoice.PaymentGatewayInvoice.PaymentGatewayType;
        if (invoiceGateway != PaymentGatewayType.Zoop)
            throw new BusinessException(ExceptionConsts.Invoice.InvalidInvoiceGateway);

        var gateway = _paymentGatewayFactory.GetPaymentGateway(invoice.PaymentGatewayInvoice.PaymentGatewayType);
        await gateway.GeneratePix(invoice);
        return await _invoiceRepository.UpdateAsync(invoice);
    }

    /// <summary>
    /// Responsável por gerar o código indentificador único da fatura
    /// </summary>
    /// <param name="school">Escola</param>
    /// <param name="referenceDate">Data de referência</param>
    /// <return>Código identificador único gerado</return>
    private async Task<string> GetUniqueIdentifierCode(SchoolCacheItem school, string referenceDate)
    {
        var yearMonth = $"{referenceDate[2..4]}{referenceDate[5..]}";
        var unique = RandomCodeGenerator.GenerateCodeAlphaNumeric(5);
        var code = $"{school.Code}-{yearMonth}-{unique}";

        var codeExists = await _invoiceRepository.AnyAsync(x => x.Code == code);

        while (codeExists)
        {
            unique = RandomCodeGenerator.GenerateCodeAlphaNumeric(5);
            code = $"{school.Code}-{yearMonth}-{unique}";

            codeExists = await _invoiceRepository.AnyAsync(x => x.Code == code);
        }

        return code;
    }

    private async Task<SchoolCacheItem> TryGetSchoolByCnpj(Document companyCnpj)
    {
        var school = await _schoolManager.GetSchoolByCNPJ(companyCnpj.Value);
        if (school == null)
            throw new BusinessException(ExceptionConsts.School.NotFoundWithGivenDocument)
                .WithData("cnpj", companyCnpj.ValueMasked);
        return school;
    }

    private async Task CheckExternalId(string externalId)
    {
        var checkExternalId = await _settingProvider.GetAsync<bool>(EducbankSettings.ValidateInvoiceExternalId);
        if (checkExternalId && externalId != null && await _invoiceRepository.AnyAsync(x =>
                x.ExternalId == externalId && (x.PaymentGatewayInvoices == null
                                               || (x.PaymentGatewayInvoices[0].State !=
                                                   PaymentGatewayInvoiceState.Canceled &&
                                                   x.PaymentGatewayInvoices[0].State !=
                                                   PaymentGatewayInvoiceState.Error))))
            throw new BusinessException(ExceptionConsts.Invoice.DuplicateExternalId)
                .WithData("ExternalId", externalId);
    }

    private static PaymentGatewayInvoice TryGetPaymentGatewayInvoice(Invoice invoice, Guid paymentGatewayinvoiceId)
    {
        var paymentGatewayInvoice =
            invoice.PaymentGatewayInvoices.FirstOrDefault(x => x.Id == paymentGatewayinvoiceId);
        if (paymentGatewayInvoice == null)
            throw new BusinessException(ExceptionConsts.Invoice.PaymentGatewayInvoiceNotFound);

        return paymentGatewayInvoice;
    }

    private async Task<PaymentFine> GetPaymentFineFromSettings()
    {
        var allowOverduePayment = await _settingProvider.GetAsync<bool>(EducbankSettings.AllowOverduePayment);
        var isEnable = await _settingProvider.GetAsync<bool>(EducbankSettings.IsEnablePaymentFine);

        if (!allowOverduePayment || !isEnable)
            return null;

        var overdueFine = await _settingProvider.GetAsync<decimal>(EducbankSettings.OverdueFine);
        var dailyInterest = await _settingProvider.GetAsync<decimal>(EducbankSettings.DailyInterest);

        var paymentFine = new PaymentFine(overdueFine, dailyInterest);

        return paymentFine;
    }

    private async Task<string> GetInvoiceUrl(Guid invoiceId)
    {
        var invoiceBaseUrl = await GetInvoiceBaseUrl();

        var url = invoiceBaseUrl.EnsureEndsWith('/') + "get-bankslip/" + invoiceId;

        return url;
    }

    public async Task<string> GetInvoiceBaseUrl()
    {
        var tenantName = await GetTenantName();

        var url = _configuration["App:WebUrl"];
        var requireHttps = _configuration["App:RequireHttpsMetadata"].To<bool>();

        if (requireHttps)
            url = "https://" + tenantName + "." + url;
        else
            url = "http://" + tenantName + "." + url;

        url = url.EnsureEndsWith('/') + "invoice";

        return url;
    }

    private async Task<string> GetTenantName()
    {
        if (CurrentTenant.Id == null)
            return "admin";

        if (!MapTenantNames.TryGetValue(CurrentTenant.Id.Value, out string tenantName))
        {
            var tenant = await _tenantStore.FindAsync(CurrentTenant.Id.Value);
            tenantName = tenant.Name;
            MapTenantNames[CurrentTenant.Id.Value] = tenantName;
        }

        return tenantName;
    }

    public async Task GerarRemessaCancelamento(Invoice invoice)
    {
        // Se a fatura NÃO foi criada por uma arquivo de remessa SantanderCnab
        // OU se já foi cancelada por uma arquiro de remessa de cancelamento
        // OU se já foi expirada pelo banco
        // OU se uma action com o nome "GerarRemessaCancelamento" já foi adicionada à fatura
        if (invoice.PaymentGatewayInvoices.Last().PaymentGatewayType != PaymentGatewayType.SantanderCnab
            || invoice.PaymentGatewayInvoices.Last().CancelationReason is InvoiceCancelationReason.CanceledByRemessa
                or InvoiceCancelationReason.ExpiredCanceledByUser or InvoiceCancelationReason.ExpiredPaidAtSchool
            || invoice.Actions.Any(a => a.Name == Action.GerarRemessaCancelamentoActionName)
           )
        {
            return;
        }

        var customVariables = invoice.CustomVariables ?? new List<CustomVariables>();
        if (customVariables.Any())
        {
            foreach (var variable in customVariables)
            {
                if (variable.Name != BankIntegrationConsts.RemessaInvoiceCustomVariableName)
                    continue;

                var action = new Action(Action.GerarRemessaCancelamentoActionName, invoice.TenantId);

                var bankIntegrationRemessaFile =
                    await _bankIntegrationRemessaFileRepository.FirstOrDefaultAsync(x =>
                        x.Id == Guid.Parse(variable.Value));

                if (AddActionsRemessaCancelamento(invoice, bankIntegrationRemessaFile, action))
                    break;

                action.Status = StatusEnum.Failed;
                action.AddParameters("FailedMsg",
                    "bankIntegrationRemessaFile.RemessaFileBank or bankIntegrationRemessaFile.RemessaFileType is invalid");
                invoice.Actions.Add(action);
                break;
            }
        }
    }

    private static bool AddActionsRemessaCancelamento(Invoice invoice,
        BankIntegrationRemessaFile bankIntegrationRemessaFile, Action action)
    {
        if (bankIntegrationRemessaFile == null)
        {
            action.Status = StatusEnum.Failed;
            action.AddParameters("FailedMsg", "bankIntegrationRemessaFile is null");
            invoice.Actions.Add(action);
            return true;
        }

        action.AddParameters("RemessaFileBank", bankIntegrationRemessaFile.RemessaFileBank.ToString());
        action.AddParameters("RemessaFileType", bankIntegrationRemessaFile.RemessaFileType.ToString());
        action.AddParameters("RemessaTransmissionCode", bankIntegrationRemessaFile.Header.BankCodigoTransmissao);

        var bankIntegrationRemessaRow =
            bankIntegrationRemessaFile.Rows.Find(x => x.RelatedInvoice == invoice.Id);

        if (bankIntegrationRemessaRow == null)
        {
            action.Status = StatusEnum.Failed;
            action.AddParameters("FailedMsg", "bankIntegrationRemessaRow is null");
            invoice.Actions.Add(action);
            return true;
        }

        switch (bankIntegrationRemessaFile.RemessaFileBank)
        {
            case FileBank.Santander when
                bankIntegrationRemessaFile.RemessaFileType == FileType.CNAB240:
                action.AddParameters("Row", GerarSantanderCnab240SegmentP(bankIntegrationRemessaRow));
                invoice.Actions.Add(action);
                return true;
            case FileBank.Santander when
                bankIntegrationRemessaFile.RemessaFileType == FileType.CNAB400:
                action.AddParameters("Row", GerarSantanderCnab400Segment1Rem(bankIntegrationRemessaRow));
                invoice.Actions.Add(action);
                return true;
            default:
                return false;
        }
    }

    private async Task ValidatePaymentFine(PaymentFine paymentFine)
    {
        if (paymentFine == null) return;

        var maxDailyInterest = await _settingProvider.GetAsync<decimal>(EducbankSettings.DailyInterest);
        if (paymentFine.DailyInterest != null && paymentFine.DailyInterest != 0 &&
            paymentFine.DailyInterest > maxDailyInterest)
            throw new BusinessException(ExceptionConsts.Invoice.InvalidDailyInterestMaxValue);

        var maxOverdueFine = await _settingProvider.GetAsync<decimal>(EducbankSettings.OverdueFine);
        if (paymentFine.OverdueFine != null && paymentFine.OverdueFine != 0 &&
            paymentFine.OverdueFine > maxOverdueFine)
            throw new BusinessException(ExceptionConsts.Invoice.InvalidOverdueFineMaxValue);
    }

    private async Task ValidateEbpayPlan(IReadOnlyCollection<PlannedInstallment> plannedInstallments)
    {
        if (plannedInstallments == null || !plannedInstallments.Any())
            throw new BusinessException(ExceptionConsts.Invoice.MustHaveAtLeastOnePlannedInstallment);

        var validateEbpayPlan = await _settingProvider.GetAsync<bool>(EducbankSettings.ValidateEbpayPlan);
        if (validateEbpayPlan)
        {
            var ebpayPlan = plannedInstallments.First().EbpayPlan;

            if (plannedInstallments.Any(x => x.EbpayPlan != ebpayPlan))
                throw new BusinessException(ExceptionConsts.Invoice.AllItemsMustBelongToTheSameEbpayPlan);
        }
    }

    private static string GerarSantanderCnab240SegmentP(BankIntegrationRemessaRow bankIntegrationRemessaRow)
    {
        var segmenP = new SantanderCnab240SegmentP();
        segmenP.IdentificacaoDoBoletoNaEmpresa = bankIntegrationRemessaRow.CompanyBankSlipId;
        segmenP.IdentificacaoDoBoletoNoBanco = Convert.ToInt64(bankIntegrationRemessaRow.BankBankSlipId);
        segmenP.TipoDeCobranca = bankIntegrationRemessaRow.BankCarteira.GetHashCode().ToString();
        segmenP.CodigoDeMovimentoDaRemessa = RemessaCodigoMovimentoEnum.CancelamentoBoleto.GetHashCode();
        segmenP.NumeroDoDocumento = bankIntegrationRemessaRow.DocumentNumber;
        segmenP.DataDeVencimentoDoBoleto = Convert.ToInt32(bankIntegrationRemessaRow.DueDate.ToString("ddMMyyyy"));
        segmenP.ValorNominalDoBoleto = bankIntegrationRemessaRow.ValueCents;
        return segmenP.ToRow();
    }

    private static string GerarSantanderCnab400Segment1Rem(BankIntegrationRemessaRow bankIntegrationRemessaRow)
    {
        var segment1 = new SantanderCnab400Segment1Rem();
        segment1.IdentificacaoDoBoletoNaEmpresa = bankIntegrationRemessaRow.CompanyBankSlipId;
        segment1.IdentificacaoDoBoletoNoBanco = Convert.ToInt64(bankIntegrationRemessaRow.BankBankSlipId);
        segment1.TipoDeCobranca = bankIntegrationRemessaRow.BankCarteira.GetHashCode();
        segment1.CodigoDeMovimentoDaRemessa = RemessaCodigoMovimentoEnum.CancelamentoBoleto.GetHashCode();
        segment1.NumeroDoDocumento = bankIntegrationRemessaRow.DocumentNumber;
        segment1.DataDeVencimentoDoBoleto = Convert.ToInt32(bankIntegrationRemessaRow.DueDate.ToString("ddMMyy"));
        segment1.ValorNominalDoBoleto = bankIntegrationRemessaRow.ValueCents;
        segment1.TipoDeInscricaoDoPagador = 1;
        segment1.NumeroDeInscricaoDoPagador = Convert.ToInt64(bankIntegrationRemessaRow.Payer?.Document.Value);
        segment1.NomeDoPagador = bankIntegrationRemessaRow.Payer?.Name;
        segment1.EnderecoDoPagador = bankIntegrationRemessaRow.Payer?.Address?.Street + " " +
                                     bankIntegrationRemessaRow.Payer?.Address?.Number + " " +
                                     bankIntegrationRemessaRow.Payer?.Address?.Complement;
        segment1.BairroDoPagador = bankIntegrationRemessaRow.Payer?.Address?.District;
        segment1.CepDoPagador = Convert.ToInt32(bankIntegrationRemessaRow.Payer?.Address?.ZipCode[..5]);
        segment1.SufixoDoCepDoPagador = Convert.ToInt32(bankIntegrationRemessaRow.Payer?.Address?.ZipCode[6..9]);
        segment1.CidadeDoPagador = bankIntegrationRemessaRow.Payer?.Address?.City;
        segment1.UnidadeDeFederacaoDoPagador = bankIntegrationRemessaRow.Payer?.Address?.State;
        return segment1.ToRow();
    }

    private static string CheckReferenceDate(DateTime dueDate, string referenceDate)
    {
        if (referenceDate.IsNullOrEmpty())
        {
            referenceDate = $"{dueDate.Year:0000}-{dueDate.Month:00}";
        }

        if (referenceDate.Length != 7)
        {
            throw new BusinessException(ExceptionConsts.Invoice.ReferenceDateLength);
        }

        return referenceDate;
    }

    private async Task<Invoice> getInvoice(Guid id)
    {
        var invoice = await _invoiceRepository.FirstOrDefaultAsync(x => x.Id == id);

        if (invoice == null)
            throw new BusinessException(ExceptionConsts.Invoice.NotFound);

        // If no gateway invoice exists for this invoice or if it is a local invoice, there is nothing to do
        if (invoice.PaymentGatewayInvoice == null
            || invoice.PaymentGatewayInvoice.PaymentGatewayType == PaymentGatewayType.Local)
        {
            return null;
        }

        return invoice;
    }

    public async Task ValidateFineAndCharges(Invoice invoice)
    {
        var ipcaChargeEnabled = await _settingProvider.GetAsync<bool>(EducbankSettings.IpcaTaxCharge);

        await ApplyIpca(invoice, ipcaChargeEnabled);
    }

    public async Task ApplyIpca(Invoice invoice, bool chargeIpca)
    {
        var minimumOverduedDaysToChargeIpca = await
            _settingProvider.GetAsync<int>(EducbankSettings.MinimumOverduedDaysToChargeIpca);
        var minimumDateToChargeIpca = EbClock.GetBrasiliaToday().AddDays(-minimumOverduedDaysToChargeIpca);
        var inflationTaxes = await _inflationTaxRepository.GetListAsync(x => x.TaxType == TaxType.Ipca);

        if (chargeIpca && invoice.DueDate.Date < minimumDateToChargeIpca.Date)
        {
            invoice.SetIpcaFine(
                new IpcaFine(
                    invoice.DueDate,
                    inflationTaxes,
                    invoice.ValueToCalculateTransfer
                ));
        }
    }

    private async Task<RemoteInvoicePaymentDetailsDto> getDetalhes(Invoice invoice)
    {
        var gateway = _paymentGatewayFactory
            .GetPaymentGateway(invoice.PaymentGatewayInvoice.PaymentGatewayType);

        var details = await gateway.GetPaymentDetails(invoice, invoice.PaymentGatewayInvoice.RemoteId);

        // If the invoice was not paid, do not update payment data
        PaymentGatewayInvoiceState[] invoiceStates =
        {
            PaymentGatewayInvoiceState.Paid,
            PaymentGatewayInvoiceState.PartiallyPaid
        };

        if (!invoiceStates.Contains(details.State))
        {
            return null;
        }

        if (details.PaymentDate == null)
            throw new BusinessException(ExceptionConsts.Invoice.InvalidOrNullPaymentDate);

        return details;
    }

    public async Task<Invoice> CheckForPaymentAtConciliation(Guid id)
    {
        var invoice = await getInvoice(id);
        if (invoice == null)
        {
            return null;
        }

        var details = await getDetalhes(invoice);
        if (details == null)
        {
            return null;
        }

        long? effectiveDiscountCents = null;
        long? effectiveTotalFineCents = null;
        if (invoice.PaymentGatewayInvoice.PaymentGatewayType == PaymentGatewayType.Iugu)
        {
            effectiveDiscountCents = details.TotalCents - details.TotalPaidCents;
            if (effectiveDiscountCents < 0)
                effectiveDiscountCents = 0;

            effectiveTotalFineCents = details.EffectiveTotalFineCents;
        }

        PayConciliationCommand updateCommand = new PayConciliationCommand(
            invoice.PaymentGatewayInvoice.Id, details.PaymentDate,
            details.CommissionCents, details.TaxesPaidCents, details.TotalPaidCents,
            details.PaymentMethod, details.GatewayPaymentDate, effectiveDiscountCents,
            effectiveTotalFineCents);

        await PayAtConciliation(invoice, updateCommand);

        return await _invoiceRepository.UpdateAsync(invoice);
    }

    public async Task CheckForPayment(Guid id)
    {
        var invoice = await getInvoice(id);
        if (invoice == null)
        {
            return;
        }

        var details = await getDetalhes(invoice);
        if (details == null)
        {
            return;
        }

        long? effectiveDiscountCents = null;
        long? effectiveTotalFineCents = null;
        if (invoice.PaymentGatewayInvoice.PaymentGatewayType == PaymentGatewayType.Iugu)
        {
            effectiveDiscountCents = details.TotalCents - details.TotalPaidCents;
            if (effectiveDiscountCents < 0)
                effectiveDiscountCents = 0;

            effectiveTotalFineCents = details.EffectiveTotalFineCents;
        }

        PayInvoiceCommand updateCommand = new PayInvoiceCommand(
            invoice.PaymentGatewayInvoice.Id, details.PaymentDate,
            details.CommissionCents, details.TaxesPaidCents, details.TotalPaidCents,
            details.PaymentMethod, details.GatewayPaymentDate, effectiveDiscountCents,
            effectiveTotalFineCents);

        await Pay(invoice, updateCommand);

        await _invoiceRepository.UpdateAsync(invoice);
    }

    private async Task ValidateCreditCardPayment(Invoice invoice, int numberOfInstallments)
    {
        var paymentByCreditCardIsEnable = await _settingProvider.GetAsync<bool>(EducbankSettings.CreditCardPayment);
        if (!paymentByCreditCardIsEnable)
            throw new BusinessException(ExceptionConsts.Invoice.PaymentByCreditCardNotEnable);

        var paymentByCreditCardOnlyToOverdueInvoices =
            await _settingProvider.GetAsync<bool>(EducbankSettings.CreditCardPaymentOnlyToOverdueInvoices);

        var invoiceIsOverdueByBaseDueDateSpecification = new InvoiceIsOverdueByBaseDueDateSpecification();
        var invoiceIsOverdue = invoiceIsOverdueByBaseDueDateSpecification.IsSatisfiedBy(invoice);

        if (paymentByCreditCardOnlyToOverdueInvoices && !invoiceIsOverdue)
            throw new BusinessException(ExceptionConsts.Invoice.PaymentByCreditCardEnableOnlyToOverdueInvoices);

        var maxNumberOfInstallment =
            await _settingProvider.GetAsync<int>(EducbankSettings.MaxOfCreditCardInstallments);

        if (numberOfInstallments > maxNumberOfInstallment)
            throw new BusinessException(ExceptionConsts.Invoice.MaxOfCreditCardInstallments);
    }

    public async Task<long> GetCreditCardChargesCents(Invoice invoice, long value, byte numberOfInstallments,
        CardBrand brand)
    {
        var creditCardTax = await GetGatewayPlan(invoice, brand);

        var fee = creditCardTax.Fees.Find(f => f.InstallmentNumber.Equals(numberOfInstallments));

        if (fee == null)
            throw new BusinessException(ExceptionConsts.Invoice.PaymentPlanNotFound);

        var result = Convert.ToInt64(value * fee.Percentual);

        return result;
    }

    public async Task<GatewayPlan> GetGatewayPlan(Invoice invoice, CardBrand cardBrand)
    {
        GatewayPlan gatewayPlan;
        if (invoice.EbpayPlan.Equals(CalculatorStrategyEnum.ZeroDefault))
        {
            using (_multiTenantDataFilter.Disable())
            {
                gatewayPlan = await _gatewayPlanRepository.FirstOrDefaultAsync(f =>
                    f.CardBrand.Equals(cardBrand) &&
                    f.PaymentGatewayType.Equals(PaymentGatewayType.Zoop) &&
                    f.PaymentMethod.Equals(InvoicePaymentMethod.CreditCard) &&
                    f.TenantId == null);
            }
        }
        else
        {
            gatewayPlan = await _gatewayPlanRepository.FirstOrDefaultAsync(f =>
                f.CardBrand.Equals(cardBrand) &&
                f.PaymentGatewayType.Equals(PaymentGatewayType.Zoop) &&
                f.PaymentMethod.Equals(InvoicePaymentMethod.CreditCard) &&
                f.TenantId.Equals(CurrentTenant.Id));
        }

        if (gatewayPlan == null)
            throw new AbpException("GatewayPlan not found");

        return gatewayPlan;
    }

    public async Task<List<Invoice>> GetInvoicesToCancelByCanceledEnrollment(Enrollment canceledEnrollment,
        DateTime cancellationDateTime)
    {
        var query = (await _invoiceRepository.GetCollectionAsync()).AsQueryable();

        query = query.Where(invoice => invoice.PlannedInstallments.Any(item =>
            item.EnrollmentId.HasValue && item.EnrollmentId.Value == canceledEnrollment.Id));

        var invoices = await AsyncExecuter.ToListAsync(query);

        return invoices
            .Where(invoice => invoice.DateToCalculateTransfer.Date >= cancellationDateTime.Date
                              && invoice.State.Equals(PaymentGatewayInvoiceState.Pending))
            .ToList();
    }

    public async Task<List<Invoice>> WriteCancellationReEnrollmentTransactions(Enrollment previousEnrollment)
    {
        var query = (await _invoiceRepository.GetCollectionAsync()).AsQueryable();

        query = query.Where(invoice => invoice.PlannedInstallments.Any(item =>
            item.EnrollmentId.HasValue && item.EnrollmentId.Value == previousEnrollment.Id));

        query = query.Where(x =>
            x.Transactions.Any(t => t.TransactionType.Equals(TransactionType.ReEnrollment) && !t.IsCanceled));

        var invoices = await AsyncExecuter.ToListAsync(query);

        var result = new List<Invoice>();

        foreach (var invoice in invoices)
        {
            var lastReEnrollmentTransaction = invoice.Transactions.OrderBy(x => x.CreationTime)
                .LastOrDefault(x => x.TransactionType.Equals(TransactionType.ReEnrollment) && !x.IsCanceled);

            if (lastReEnrollmentTransaction == null)
                continue;

            var isAlreadyCanceled = invoice.Transactions.Any(x => !x.IsCanceled
                                                                  && x.TransactionType.Equals(TransactionType
                                                                      .ReEnrollmentCanceled)
                                                                  && x.ReferenceId.HasValue
                                                                  && x.ReferenceId.Value.Equals(
                                                                      lastReEnrollmentTransaction.Id));
            if (isAlreadyCanceled)
                continue;

            result.Add(invoice);
        }

        var zeroDefaultStrategy = _ebpayCalculatorStrategy.GetStrategy(CalculatorStrategyEnum.ZeroDefault);
        foreach (var invoice in result)
        {
            var eventId = GuidGenerator.Create();
            invoice.AddEvent(InvoiceEventType.CancelReEnrollment, eventId);
            await zeroDefaultStrategy.AddInvoiceCancelReEnrollmentTransactions(invoice, EbClock.Now, eventId);
            var enrollmentIds = invoice.PlannedInstallments.Select(x=> x.EnrollmentId.Value);
            var enrollments = await _enrollmentRepository.GetListAsync(x=> enrollmentIds.Contains(x.Id));
            var guardians = enrollments.SelectMany(x=> x.Guardians);
            var guardianNames = string.Join(',', guardians.Select(x=> x.Name));
            invoice.AddCancelReEnrollmentEvent(eventId, guardianNames, InvoiceEventType.CancelReEnrollment.ToString());
        }

        return result;
    }

    public async Task<List<Invoice>> WriteReEnrollmentRetentionTransactionsForEnrollment(
        Enrollment previousEnrollment)
    {
        var query = (await _invoiceRepository.GetCollectionAsync()).AsQueryable();

        query = query.Where(invoice => invoice.PlannedInstallments.Any(item =>
            item.EnrollmentId.HasValue && item.EnrollmentId.Value == previousEnrollment.Id));

        query = query.Where(item => item.DateToCalculateTransfer < EbClock.Today);

        query = query.Where(item =>
            item.PaymentGatewayInvoices[0].State == PaymentGatewayInvoiceState.Expired ||
            item.PaymentGatewayInvoices[0].State == PaymentGatewayInvoiceState.Pending);

        var invoices = await AsyncExecuter.ToListAsync(query);

        var result = new List<Invoice>();
        var zeroDefaultStrategy = _ebpayCalculatorStrategy.GetStrategy(CalculatorStrategyEnum.ZeroDefault);

        foreach (var invoice in invoices)
        {
            var educbankBalance = invoice.EducbankBalance;
            var schoolBalance = invoice.Balance;

            if ((educbankBalance * -1) != schoolBalance)
            {
                result.Add(invoice);
            }

            var eventId = GuidGenerator.Create();
            invoice.AddEvent(InvoiceEventType.ReEnrollment, eventId);
            await zeroDefaultStrategy.AddInvoiceReEnrollmentTransactions(invoice, EbClock.Now, eventId);
            var enrollmentIds = invoice.PlannedInstallments.Select(x=> x.EnrollmentId.Value);
            var enrollments = await _enrollmentRepository.GetListAsync(x=> enrollmentIds.Contains(x.Id));
            var guardians = enrollments.SelectMany(x=> x.Guardians);
            var guardianNames = string.Join(',', guardians.Select(x=> x.Name));
            invoice.AddReEnrollmentEvent(eventId, guardianNames, InvoiceEventType.ReEnrollment.ToString());
        }

        return result;
    }

    public async Task AddNewEnrollmentByGuardianDocumentTransaction(List<Invoice> invoices, Guid newEnrollmentId)
    {
        var zeroDefaultStrategy = _ebpayCalculatorStrategy.GetStrategy(CalculatorStrategyEnum.ZeroDefault);
        foreach (var invoice in invoices)
        {
            var eventId = GuidGenerator.Create();
            invoice.AddEvent(InvoiceEventType.ReEnrollment, eventId);
            await zeroDefaultStrategy.AddInvoiceNewEnrollmentByGuardianDocumentTransaction(invoice, EbClock.Now, newEnrollmentId, eventId);
            var enrollmentIds = invoice.PlannedInstallments.Select(x=> x.EnrollmentId.Value);
            var enrollments = await _enrollmentRepository.GetListAsync(x=> enrollmentIds.Contains(x.Id));
            var guardians = enrollments.SelectMany(x=> x.Guardians);
            var guardianNames = string.Join(',', guardians.Select(x=> x.Name));
            invoice.AddReEnrollmentEvent(eventId, guardianNames, InvoiceEventType.ReEnrollment.ToString());
        }
    }

    public async Task AddNewInvoiceByGuardianDocumentTransaction(List<Invoice> invoices, string invoiceCode)
    {
        var zeroDefaultStrategy = _ebpayCalculatorStrategy.GetStrategy(CalculatorStrategyEnum.ZeroDefault);
        foreach (var invoice in invoices)
        {
            var eventId = GuidGenerator.Create();
            invoice.AddEvent(InvoiceEventType.ReEnrollment, eventId);
            await zeroDefaultStrategy.AddInvoiceNewInvoiceByGuardianDocumentTransaction(invoice, EbClock.Now, invoiceCode, eventId);
            var enrollmentIds = invoice.PlannedInstallments.Select(x=> x.EnrollmentId.Value);
            var enrollments = await _enrollmentRepository.GetListAsync(x=> enrollmentIds.Contains(x.Id));
            var guardians = enrollments.SelectMany(x=> x.Guardians);
            var guardianNames = string.Join(',', guardians.Select(x=> x.Name));
            invoice.AddReEnrollmentEvent(eventId, guardianNames, InvoiceEventType.ReEnrollment.ToString());
        }
    }


    private async Task<bool> ValidateGatewayProduct(List<PlannedInstallment> plannedInstallmentsList)
    {
        if (plannedInstallmentsList != null &&
            plannedInstallmentsList.Any(x => x.EbpayPlan == CalculatorStrategyEnum.Gateway))
            return await _settingProvider.GetAsync<bool>(EducbankSettings.EnableGatewayProduct);

        return true;
    }
}
