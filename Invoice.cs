using Educbank.Core.CalculatorStrategies;
using Educbank.Core.PaymentGateway;
using Educbank.Core.PlannedInstallments;
using Educbank.Core.ValueObjects;
using Educbank.Exceptions;
using Educbank.Extensions;
using Educbank.Timing;
using Educbank.ValueObjetcs;
using Microsoft.AspNetCore.Mvc;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Volo.Abp;
using Volo.Abp.Domain.Entities.Auditing;
using Volo.Abp.MultiTenancy;
using Action = Educbank.Core.Actions.Action;
using ValueObject = Volo.Abp.Domain.Values.ValueObject;

namespace Educbank.Core.Invoices
{
    public sealed class Invoice : FullAuditedAggregateRoot<Guid>, IMultiTenant, IInvoiceConfig
    {
        /// <summary>
        /// Invoice unique identifier code
        /// </summary>
        [Required]
        public string Code { get; private set; }

        public string Url { get; private set; }

        public string ExternalId { get; set; }

        public Guid? TenantId { get; private set; }

        public Document CompanyCnpj { get; private set; }

        public string CompanyName { get; private set; }

        /// <summary>
        /// Data de competência (YYYY-MM)
        /// </summary>
        [Required]
        public string ReferenceDate { get; private set; }

        public List<InvoiceLog> InvoiceLogs { get; private set; } = new();

        public ICollection<Guid> SupersededBy { get; internal set; } = new List<Guid>();

        public DateTime? SupersedeDate { get; internal set; }

        [BsonDateTimeOptions(DateOnly = true)] public DateTime? CloseTime { get; private set; }

        public Guid? SupersedeUserId { get; internal set; }

        public InvoiceSupersedeReason? SupersedeReason { get; internal set; }

        public string SupersedDescription { get; internal set; }

        public List<CustomVariables> CustomVariables { get; internal set; } = new();

        public Payer Payer { get; private set; }

        [BsonDateTimeOptions(DateOnly = true)]
        //[DisableDateTimeNormalization]
        public DateTime DueDate { get; private set; }

        [BsonDateTimeOptions(DateOnly = true)]
        //[DisableDateTimeNormalization]
        public DateTime EffectiveDueDate { get; private set; }

        [BsonDateTimeOptions(DateOnly = true)] public DateTime? TransferDateRescheduled { get; set; }

        public PaymentFine PaymentFine { get; private set; }

        public List<PlannedInstallment> PlannedInstallments { get; private set; } = new();

        public List<EarlyPaymentDiscount> EarlyPaymentDiscounts { get; private set; } = new();

        public Guid? UpdatedBy { get; private set; }

        public Guid? UpdateThe { get; private set; }

        /// <summary>
        /// Indica se a fatura está vencida.
        /// É checado primeiro se há boleto, caso sim, é verificado se está vencido com base no payment gateway invoice.
        /// </summary>
        public bool IsOverdue => PaymentGatewayInvoice?.IsOverdue() ?? this.IsOverdue();

        [HiddenInput(DisplayValue = false)] public IpcaFine IpcaFine { get; private set; } = new();

        public List<CalculatedCharges> CalculatedCharges { get; }

        public List<PaymentGatewayInvoice> PaymentGatewayInvoices { get; set; } = new();

        public Guid SchoolId { get; private set; }

        /** Computed PaymentGatewayInvoice fields **/

        public PaymentGatewayInvoice PaymentGatewayInvoice => PaymentGatewayInvoices.FirstOrDefault();

        public PaymentGatewayInvoiceState State => PaymentGatewayInvoice?.State ?? PaymentGatewayInvoiceState.Open;

        public EarlyPaymentDiscount EarlyPaymentDiscountApplicable => GetEarlyPaymentDiscountApplicable();

        public List<PaymentGatewayInvoice> PaidPaymentGatewayInvoices => GetPaidPaymentGatewayInvoices();

        public bool IsPaid => PaidPaymentGatewayInvoices.Any();

        public List<Transaction> Transactions { get; set; }

        public long Balance { get; private set; }

        public long EducbankBalance { get; private set; }

        public long ValueToCalculateTransfer { get; private set; }
        [BsonDateTimeOptions(DateOnly = true)] public DateTime DateToCalculateTransfer { get; private set; }

        [BsonDateTimeOptions(DateOnly = true)] public DateTime EffectiveDateToCalculateTransfer { get; private set; }

        public List<InvoiceEvents> Events { get; set;} = new();
        public Liquidation Liquidation { get; private set; }

        /// <summary>
        /// Get total of items in cents (net value)
        /// </summary>
        /// <returns></returns>
        public long TotalItemsCents => this.GetTotalItemsCents();

        /// <summary>
        /// Early payment discount in cents for today
        /// </summary>
        /// <returns></returns>
        public long TotalDiscountCents => IsPaid
            ? PaidPaymentGatewayInvoices.Sum(x => x.TotalDiscountCents)
            : PaymentGatewayInvoice?.TotalDiscountCents ?? this.GetApplicableDiscountCents();

        /// <summary>
        /// Fixed discount in cents
        /// </summary>
        /// <returns></returns>
        public long TotalFixedDiscountCents => PlannedInstallments.Sum(x => x.FixedDiscountCents).GetValueOrDefault(0L);

        /// <summary>
        /// Last discount in cents
        /// </summary>
        /// <returns></returns>
        public long LastDiscountCents => GetPreviousDiscountCents();

        /// <summary>
        /// Early payment discount in Percent for today
        /// </summary>
        /// <returns></returns>
        public decimal TotalDiscountPercent => GetEffectiveDiscountPercent();

        /// <summary>
        /// Sum of charges and payment fines for today
        /// </summary>
        /// <returns></returns>
        public long TotalFineCents => IsPaid
            ? PaidPaymentGatewayInvoices.Sum(x => x.TotalFineCents)
            : PaymentGatewayInvoice?.TotalFineCents + GetIpcaFineTotalCents() ??
              this.GetApplicableFineCents() + GetIpcaFineTotalCents();


        /// <summary>
        /// If has paid cents then show it, otherwise sum itens, charges and discounts
        /// </summary>
        public long TotalCents => TotalPaidCents ?? TotalItemsCents + TotalFineCents - TotalDiscountCents;

        public long OriginalTotalCents => TotalItemsCents + TotalFineCents - TotalDiscountCents;

        public long? TotalPaidCents => PaidPaymentGatewayInvoices.Any()
            ? PaidPaymentGatewayInvoices.Sum(x => x.TotalPaidCents)
            : null;

        public DateTime? PaymentDate => PaymentGatewayInvoice?.PaymentDate;


        /** Computed fields **/

        public InvoicePaymentMethod? PaidMethod => IsPaid ? PaidPaymentGatewayInvoices.First().PaidMethod : null;

        public string InvoiceState => State.ToString();

        public bool HasEarlyPaymentDiscounts => EarlyPaymentDiscounts.Any();

        public bool HasPaymentFine => PaymentFine != null;

        public bool IsClosed => PaymentGatewayInvoices.Any(x => x.State != PaymentGatewayInvoiceState.Open);

        public List<Action> Actions { get; private set; } = new();

        public List<Transaction> ValidTransactions => GetValidTransactions();

        public CalculatorStrategyEnum EbpayPlan
        {
            get
            {
                try
                {
                    return PaymentGatewayInvoice?.PlannedInstallments[0].EbpayPlan
                           ?? PlannedInstallments[0].EbpayPlan
                           ?? CalculatorStrategyEnum.ZeroDefault;
                }
                catch (ArgumentOutOfRangeException)
                {
                    return CalculatorStrategyEnum.ZeroDefault;
                }
            }
        }

        internal Invoice(Guid id, Guid? tenantId, Payer payer, DateTime dueDate, string externalId,
            string referenceDate, Document companyCnpj, string companyName, string code,
            List<PlannedInstallment> plannedInstallments, List<EarlyPaymentDiscount> earlyPaymentDiscounts,
            Guid schoolId, List<PaymentGatewayInvoice> paymentGatewayInvoices = null, PaymentFine paymentFine = null,
            string url = null)
        {
            Id = id;
            TenantId = tenantId;
            ExternalId = externalId;
            CompanyCnpj = companyCnpj;
            CompanyName = companyName?.Trim();
            Transactions = new List<Transaction>();
            Code = code;
            Url = url?.Trim();
            SchoolId = schoolId;
            SetDueDate(dueDate);
            SetReferenceDate(referenceDate);
            SetPayer(payer);
            SetPlannedInstallments(plannedInstallments);
            SetPaymentFine(paymentFine);
            SetEarlyPaymentDiscounts(earlyPaymentDiscounts);
            SetPaymentGatewayInvoices(paymentGatewayInvoices);
            SetTransferBaseFields();

            AddDistributedEvent(new InvoiceCreatedEto
            {
                InvoiceId = Id,
                TenantId = TenantId,
                CreationTime = EbClock.Now
            });
        }

        private Invoice()
        {
        }

        private void SetDueDate(DateTime dueDate)
        {
            if (dueDate == default(DateTime))
                throw new BusinessException(ExceptionConsts.Invoice.InvalidDueDate);

            DueDate = dueDate.Date;
            EffectiveDueDate = EbClock.GetNextBusinessDay(DueDate);
        }

        private List<PaymentGatewayInvoice> GetPaidPaymentGatewayInvoices()
        {
            return PaymentGatewayInvoices.Where(x => x.State == PaymentGatewayInvoiceState.Paid).ToList();
        }


        internal void SetReferenceDate(string referenceDate)
        {
            if (referenceDate.Length != 7)
                throw new BusinessException(ExceptionConsts.Invoice.ReferenceDateLength);

            var regex = new Regex(@"^\d{4}-(0[1-9]|1[0-2])", RegexOptions.None, TimeSpan.FromMilliseconds(1000));
            if (!regex.IsMatch(referenceDate))
                throw new BusinessException(ExceptionConsts.Invoice.InvalidReferenceDate);

            ReferenceDate = referenceDate;
        }

        /// <summary>
        /// Close invoice and request the payment gateway invoice
        /// </summary>
        /// <param name="paymentGatewayInvoiceId"></param>
        /// <param name="paymentGatewayType"></param>
        /// <param name="gatewayAccountId"></param>
        /// <param name="createPaymentGatewayInvoiceAsync"></param>
        /// <param name="dueDate"></param>
        /// <param name="charges"></param>
        /// <param name="eventId"></param>
        internal PaymentGatewayInvoice Close(Guid paymentGatewayInvoiceId, PaymentGatewayType paymentGatewayType, Guid eventId, Guid gatewayAccountId,
            bool createPaymentGatewayInvoiceAsync = false, DateTime? dueDate = null, long? charges = null)
        {
            if (PaymentGatewayInvoices.Any(x =>
                    x.State != PaymentGatewayInvoiceState.Creating &&
                    x.State != PaymentGatewayInvoiceState.Canceled &&
                    x.State != PaymentGatewayInvoiceState.Error))
                throw new BusinessException(ExceptionConsts.Invoice
                    .CannotAddNewPaymentGatewayInvoiceAlreadyHaveOrCanceled);

            dueDate ??= DueDate;
            var earlyPaymentDiscounts = EarlyPaymentDiscounts;

            var paymentGatewayInvoice = new PaymentGatewayInvoice(paymentGatewayInvoiceId, paymentGatewayType,
                gatewayAccountId, Payer, dueDate.Value, ReferenceDate, PlannedInstallments, charges, PaymentFine,
                earlyPaymentDiscounts, createPaymentGatewayInvoiceAsync, PaymentGatewayInvoice?.BankSlip);

            PaymentGatewayInvoices.AddFirst(paymentGatewayInvoice);

            return paymentGatewayInvoice;
        }

        public void SetInvoiceCloseTime(DateTime closeTime)
        {
            if (CloseTime == null)
                CloseTime = closeTime;
        }

        /// <summary>
        /// Gerar segunda via de fatura já expirada.
        /// </summary>
        /// <param name="newDueDate">Nova data de vencimento.</param>
        /// <param name="newCharges">Encargos à serem aplicados.</param>
        /// <param name="pgf">Payment gateway factory.</param>
        /// <param name="eventId">Payment gateway factory.</param>
        /// <returns>Fatura com nova data de vencimento.</returns>
        /// <exception cref="BusinessException"></exception>
        internal async Task Duplicate(DateTime newDueDate, long newCharges, PaymentGatewayFactory pgf, Guid eventId)
        {
            if (newDueDate < EbClock.GetBrasiliaToday())
            {
                throw new BusinessException(ExceptionConsts.Invoice
                    .CannotUpdateInvoicePaymentDetailsForDateBeforeToday);
            }

            if (!IsOverdue)
            {
                throw new BusinessException(ExceptionConsts.Invoice.CannotUpdateInvoicePaymentDetailsThatIsNotOverdue);
            }

            if (newDueDate > EbClock.GetBrasiliaToday().AddDays(1))
            {
                throw new BusinessException(ExceptionConsts.Invoice
                    .CannotDuplicateInvoiceThatNewDateAfterTodayAndTomorrow);
            }

            // Cancelar atual
            PaymentGatewayInvoice.Cancel(InvoiceCancelationReason.InvoiceDuplicated);
            var oldPgiId = PaymentGatewayInvoice.Id;

            AddInvoiceLog(new InvoiceLog(new
            {
                Description = "Canceled because duplication of invoice.",
                Reason = InvoiceCancelationReason.InvoiceDuplicated
            }, null, false));

            // Criar nova payment gateway invoice
            if (newCharges > 0 && PaymentFine?.OverdueFine > 0)
            {
                PaymentFine.OverdueFine = null;
            }

            var defaultGatewayAccount = await pgf.GetDefaultGatewayAccount(TenantId, EbpayPlan);
            var newPgi = new PaymentGatewayInvoice(Guid.NewGuid(), defaultGatewayAccount.PaymentGatewayType,
                defaultGatewayAccount.Id, Payer, newDueDate, ReferenceDate, PlannedInstallments, newCharges,
                PaymentFine, null, false, PaymentGatewayInvoice?.BankSlip);

            PaymentGatewayInvoices.AddFirst(newPgi);

            var paymentGateway = pgf.GetPaymentGateway(newPgi.PaymentGatewayType);

            await paymentGateway.CreateInvoice(newPgi, this);

            if (newPgi.Errors is { Count: > 0 })
            {
                throw new EducbankBusinessException(newPgi.ErrorStatusCode, string.Join("; ", newPgi.Errors));
            }

            AddInvoiceLog(new InvoiceLog(new
            {
                Description = "Canceled because duplication of invoice.",
                Reason = InvoiceCancelationReason.InvoiceDuplicated
            }, "Segunda via emitida", false));

            AddDistributedEvent(
                new InvoiceDuplicatedEto
                {
                    InvoiceId = Id,
                    TenantId = TenantId,
                    OldPaymentGatewayInvoiceId = oldPgiId,
                    EventId = eventId
                }
            );
        }

        internal async Task UpdateDefaultGateway(DateTime newDueDate, long newCharges, PaymentGatewayFactory pgf)
        {
            if (newDueDate < EbClock.GetBrasiliaToday())
            {
                throw new BusinessException(ExceptionConsts.Invoice
                    .CannotUpdateInvoicePaymentDetailsForDateBeforeToday);
            }

            // Cancelar atual
            PaymentGatewayInvoice.Cancel(InvoiceCancelationReason.UpdateDefaultGateway);
            var oldPgiId = PaymentGatewayInvoice.Id;

            AddInvoiceLog(new InvoiceLog(new
            {
                Description = "Update because default gateway is changed.",
                Reason = InvoiceCancelationReason.UpdateDefaultGateway
            }, null, false));

            // Criar nova payment gateway invoice
            if (newCharges > 0 && PaymentFine?.OverdueFine > 0)
            {
                PaymentFine.OverdueFine = null;
            }

            var defaultGatewayAccount = await pgf.GetDefaultGatewayAccount(TenantId, EbpayPlan);
            var newPgi = new PaymentGatewayInvoice(Guid.NewGuid(), defaultGatewayAccount.PaymentGatewayType,
                defaultGatewayAccount.Id, Payer, newDueDate, ReferenceDate, PlannedInstallments, newCharges,
                PaymentFine, EarlyPaymentDiscounts, false, PaymentGatewayInvoice?.BankSlip);

            PaymentGatewayInvoices.AddFirst(newPgi);

            var paymentGateway = pgf.GetPaymentGateway(newPgi.PaymentGatewayType);

            await paymentGateway.CreateInvoice(newPgi, this);

            if (newPgi.Errors is { Count: > 0 })
            {
                throw new EducbankBusinessException(newPgi.ErrorStatusCode, string.Join("; ", newPgi.Errors));
            }

            AddInvoiceLog(new InvoiceLog(new
            {
                Description = "Update because default gateway is changed.",
                Reason = InvoiceCancelationReason.UpdateDefaultGateway
            }, "Segunda via emitida", false));

            AddDistributedEvent(
                new InvoiceDuplicatedEto
                {
                    InvoiceId = Id,
                    TenantId = TenantId,
                    OldPaymentGatewayInvoiceId = oldPgiId
                }
            );
        }

        /// <summary>
        /// Gerar segunda via de fatura já expirada.
        /// </summary>
        /// <param name="newDueDate">Nova data de vencimento.</param>
        /// <param name="newCharges">Encargos à serem aplicados.</param>
        /// <param name="pgf">Payment gateway factory.</param>
        /// <returns>Fatura com nova data de vencimento.</returns>
        /// <exception cref="BusinessException"></exception>

        internal PaymentGatewayInvoice PayAtSchool(DateTime paymentDate, Guid gatewayAccountId, long totalPaidCents, Guid eventId, long? effectiveDiscountCents = null,
            long? effectiveTotalFineCents = null)
        {
            if (State == PaymentGatewayInvoiceState.Paid)
                throw new BusinessException(ExceptionConsts.Invoice.AlreadyPaid);

            if (paymentDate.Date > EbClock.GetBrasiliaToday().Date)
            {
                throw new BusinessException(ExceptionConsts.Invoice.PaymentDateNotAllowed);
            }

            var oldPaymentGatewayInvoice = PaymentGatewayInvoice;
            if (oldPaymentGatewayInvoice != null)
            {
                var cancellationReason = State == PaymentGatewayInvoiceState.Expired
                    ? InvoiceCancelationReason.ExpiredPaidAtSchool
                    : InvoiceCancelationReason.PaidAtSchool;

                oldPaymentGatewayInvoice.Cancel(cancellationReason);
            }

            var localPaymentGatewayInvoice = new PaymentGatewayInvoice(Guid.NewGuid(), PaymentGatewayType.Local,
                gatewayAccountId, Payer, DueDate, ReferenceDate, PlannedInstallments, null, PaymentFine,
                EarlyPaymentDiscounts);
            localPaymentGatewayInvoice.Pending();

            PaymentGatewayInvoices.AddFirst(localPaymentGatewayInvoice);

            localPaymentGatewayInvoice.Pay(paymentDate, totalPaidCents, null, null, InvoicePaymentMethod.School,
                EbClock.Now, effectiveDiscountCents, effectiveTotalFineCents);

            return oldPaymentGatewayInvoice;
        }

        internal void PayByCreditCard(Guid? gatewayAccountId, long totalPaidCents, Guid eventId, long? effectiveDiscountCents = null,
            long? effectiveTotalFineCents = null, long? creditCardTax = null, string remoteId = null,
            long? gatewayFeeCents = null)
        {
            if (State.Equals(PaymentGatewayInvoiceState.Paid))
            {
                throw new BusinessException(ExceptionConsts.Invoice.AlreadyPaid);
            }

            var newPaymentGatewayInvoice = new PaymentGatewayInvoice(Guid.NewGuid(), PaymentGatewayType.Zoop,
                gatewayAccountId, Payer, DueDate, ReferenceDate, PlannedInstallments, null, PaymentFine,
                EarlyPaymentDiscounts);

            newPaymentGatewayInvoice.Pending(remoteId: remoteId);

            PaymentGatewayInvoices.AddFirst(newPaymentGatewayInvoice);

            newPaymentGatewayInvoice.Pay(EbClock.Now, totalPaidCents, gatewayFeeCents, null,
                InvoicePaymentMethod.CreditCard, EbClock.Now, effectiveDiscountCents, effectiveTotalFineCents,
                creditCardTax);
        }

        /// <summary>
        /// Cancel an invoice and request the cancellation on payment gateway
        /// </summary>
        /// <param name="cancellationReason"></param>
        /// <param name="eventId"></param>
        /// <param name="gatewayAccountId">Id da conta do gateway aonde deverá ser cancelada</param>
        internal void Cancel(InvoiceCancelationReason cancellationReason, Guid gatewayAccountId, Guid eventId)
        {
            if (State == PaymentGatewayInvoiceState.Canceled)
                throw new BusinessException(ExceptionConsts.Invoice.AlreadyCanceled);

            if (PaymentGatewayInvoice == null)
            {
                var paymentGatewayInvoice = new PaymentGatewayInvoice(Guid.NewGuid(), PaymentGatewayType.Local,
                    gatewayAccountId, Payer, DueDate, ReferenceDate, PlannedInstallments, null, PaymentFine,
                    EarlyPaymentDiscounts);

                PaymentGatewayInvoices.Add(paymentGatewayInvoice);
            }

            foreach (var paymentGatewayInvoice in PaymentGatewayInvoices)
            {
                paymentGatewayInvoice.Cancel(cancellationReason);
            }
        }

        /// <summary>
        /// Cancel all payments at school
        /// </summary>
        /// <param name="cancelationReason"></param>
        internal void CancelPaymentsAtSchool(InvoiceCancelationReason cancelationReason)
        {
            var paymentGatewayInvoices = PaymentGatewayInvoices.FindAll(x =>
                x.State == PaymentGatewayInvoiceState.Paid
                && x.PaidMethod == InvoicePaymentMethod.School);

            foreach (var paymentGatewayInvoice in paymentGatewayInvoices)
            {
                paymentGatewayInvoice.CancelPaymentAtSchool(cancelationReason);
            }
        }

        internal void RollbackCancelPaymentsAtSchool()
        {
            var paymentGatewayInvoice = PaymentGatewayInvoices.First();

            if (paymentGatewayInvoice.State.Equals(PaymentGatewayInvoiceState.Paid) &&
                paymentGatewayInvoice.PaidMethod.Equals(InvoicePaymentMethod.School))
                PaymentGatewayInvoices.RemoveAt(0);

            PaymentGatewayInvoices.First().RollbackCancel();
        }

        internal void Expire(Guid paymentGatewayInvoiceId)
        {
            var paymentGatewayInvoice = PaymentGatewayInvoices.FirstOrDefault(x => x.Id == paymentGatewayInvoiceId);

            if (paymentGatewayInvoice == null)
                throw new BusinessException(ExceptionConsts.Invoice.PaymentGatewayInvoiceAssociatedToInvoiceNotFound);

            if (paymentGatewayInvoice.State == PaymentGatewayInvoiceState.Expired)
                return;

            paymentGatewayInvoice.Expire();

            AddDistributedEvent(
                new InvoiceExpiredEto
                {
                    InvoiceId = Id,
                    TenantId = TenantId
                }
            );
        }

        internal void PayAtConciliation(PaymentGatewayInvoice paymentGatewayInvoice, DateTime paymentDate, long? totalPaidCents, Guid eventId,
            long? taxesPaidCents = null, long? commissionCents = null, InvoicePaymentMethod? paymentMethod = null,
            DateTime? gatewayPaymentDate = null, long? effectiveDiscountCents = null,
            long? effectiveTotalFineCents = null)
        {
            if (paymentGatewayInvoice == null)
                throw new BusinessException(ExceptionConsts.Invoice.PaymentGatewayInvoiceAssociatedToInvoiceNotFound);

            CheckCurrentPaymentGatewayInvoice(paymentGatewayInvoice.Id, totalPaidCents, effectiveDiscountCents,
                effectiveTotalFineCents, out var realDiscountCents, out var realTotalFineCents);

            PaymentGatewayInvoices.MoveItem(x => x.Id == paymentGatewayInvoice.Id, 0);

            paymentGatewayInvoice.PayAtConciliation(paymentDate, totalPaidCents, taxesPaidCents, commissionCents,
                paymentMethod,
                gatewayPaymentDate, realDiscountCents, realTotalFineCents);
        }

        internal void Pay(PaymentGatewayInvoice paymentGatewayInvoice, DateTime paymentDate, long? totalPaidCents, Guid eventId,
            long? taxesPaidCents = null, long? commissionCents = null, InvoicePaymentMethod? paymentMethod = null,
            DateTime? gatewayPaymentDate = null, long? effectiveDiscountCents = null,
            long? effectiveTotalFineCents = null)
        {
            if (paymentGatewayInvoice == null)
                throw new BusinessException(ExceptionConsts.Invoice.PaymentGatewayInvoiceAssociatedToInvoiceNotFound);

            CheckCurrentPaymentGatewayInvoice(paymentGatewayInvoice.Id, totalPaidCents, effectiveDiscountCents,
                effectiveTotalFineCents, out var realDiscountCents, out var realTotalFineCents);

            PaymentGatewayInvoices.MoveItem(x => x.Id == paymentGatewayInvoice.Id, 0);

            paymentGatewayInvoice.Pay(paymentDate, totalPaidCents, taxesPaidCents, commissionCents, paymentMethod,
                gatewayPaymentDate, realDiscountCents, realTotalFineCents);
        }

        /// <summary>
        /// Método responsável por checar se a fatura que está sendo paga é a atual ou não. Pode ocorrer de estar sendo
        /// paga uma fatura que já foi atualizada, sendo assim é necessário atualizar os valores reais da fatura.
        /// </summary>
        /// <param name="paymentGatewayInvoicePaidId">Id da fatura à ser paga</param>
        /// <param name="totalPaidCents">Valor pago</param>
        /// <param name="effectiveTotalFineCents">Valor dos encargos com base na fatura que está sendo paga</param>
        /// <param name="realDiscountCents">Valor dos descontos com base na última fatura movimentação da fatura</param>
        /// <param name="realTotalFineCents">Valor dos encargos com base na última fatura movimentação da fatura</param>
        /// <param name="effectiveDiscountCents">Valor de desconto com base a fatura que está sendo paga</param>
        private void CheckCurrentPaymentGatewayInvoice(Guid paymentGatewayInvoicePaidId, long? totalPaidCents,
            long? effectiveDiscountCents, long? effectiveTotalFineCents, out long? realDiscountCents,
            out long? realTotalFineCents)
        {
            realDiscountCents = effectiveDiscountCents;
            realTotalFineCents = effectiveTotalFineCents;

            // Caso a fatura à ser paga seja a mais atualizada, não fazer nada
            if (paymentGatewayInvoicePaidId.Equals(PaymentGatewayInvoice.Id))
                return;

            var totalCents = OriginalTotalCents;
            var totalFineCents = (totalPaidCents - totalCents).GetValueOrDefault(0);

            if (totalFineCents < 0)
                totalFineCents = 0;

            realTotalFineCents = totalFineCents + PaymentGatewayInvoice.Charges.GetValueOrDefault(0);
            realDiscountCents = totalPaidCents < totalCents ? totalCents - totalPaidCents : 0;
        }

        internal void Error(Guid paymentGatewayInvoiceId, List<string> errors, string rawData)
        {
            var paymentGatewayInvoice = PaymentGatewayInvoices.FirstOrDefault(x => x.Id == paymentGatewayInvoiceId);

            if (paymentGatewayInvoice == null)
                throw new BusinessException(ExceptionConsts.Invoice.PaymentGatewayInvoiceAssociatedToInvoiceNotFound);

            paymentGatewayInvoice.Error(errors, rawData);
        }

        internal void AddInvoiceLog(InvoiceLog invoiceLog)
        {
            InvoiceLogs.Add(invoiceLog);
        }

        internal void AddCustomVariable(string name, string value)
        {
            CustomVariables.Add(new CustomVariables { Name = name?.Trim(), Value = value?.Trim() });
        }

        internal void Update(DateTime duedate, PaymentFine paymentFine, Payer payer,
            List<PlannedInstallment> plannedInstallments, List<EarlyPaymentDiscount> earlyPaymentDiscounts,
            long? charges, Guid eventId)
        {
            if (State == PaymentGatewayInvoiceState.Paid)
                throw new BusinessException(ExceptionConsts.Invoice.CannotUpdatePaidInvoice);

            DueDate = duedate;
            SetPayer(payer);
            SetPlannedInstallments(plannedInstallments);
            SetPaymentFine(paymentFine);
            SetEarlyPaymentDiscounts(earlyPaymentDiscounts);
            SetTransferBaseFields();
        }

        /// <summary>
        /// Atualizar pagador da fatura
        /// </summary>
        /// <param name="doc">documento do pagador</param>
        /// <param name="phone">Novo número de telefone do pagador</param>
        /// <param name="name">Novo nome do pagador</param>
        /// <param name="email">Novo e-mail do pagador</param>
        /// <param name="address">Novo endereço do pagador</param>
        internal void UpdatePayer(Document doc, string phone, string name, string email, Address address)
        {
            // Para status open é permidido alterar o pagador
            if (State.Equals(PaymentGatewayInvoiceState.Open))
            {
                Payer = new Payer
                {
                    Name = name,
                    Document = doc,
                    Phone = phone,
                    Email = email,
                    Address = address,
                    ExtraProperties = Payer.ExtraProperties
                };
            }
            // Para status diferente de open a fatura já foi gerado, só é permitido a alteração do telefone, pois o
            // mesmo não se encontra na fatura
            else if (Payer.Document.ValueEquals(doc))
            {
                Payer.Phone = phone;
            }

            if (PaymentGatewayInvoices.Any())
            {
                foreach (var pgi in PaymentGatewayInvoices.Where(x =>
                             x.Payer != null && x.Payer.Document.Value.Equals(doc.Value)))
                {
                    pgi.UpdatePayer(phone);
                }
            }
        }

        public void AddEvent(InvoiceEventType eventType, Guid id, DateTime? creationTime = null)
        {
            Events.Add(new InvoiceEvents(id, eventType, creationTime));
        }

        private void SetPlannedInstallments(List<PlannedInstallment> plannedInstallments)
        {
            if (plannedInstallments == null || !plannedInstallments.Any())
                throw new BusinessException(ExceptionConsts.Invoice.MustHaveAtLeastOnePlannedInstallment);

            if (plannedInstallments.Any(item => plannedInstallments.Count(x => x.Id == item.Id) > 1))
            {
                throw new BusinessException(ExceptionConsts.Invoice.DuplicatedPlannedInstalmentFound);
            }

            if (PlannedInstallments != null && PlannedInstallments.Any(x =>
                    x.EbpayPlan.GetValueOrDefault(CalculatorStrategyEnum.ZeroDefault) != plannedInstallments.First()
                        .EbpayPlan.GetValueOrDefault(CalculatorStrategyEnum.ZeroDefault)))
                throw new BusinessException(ExceptionConsts.Invoice.CannotChangeEbpayPlanOfInvoice);

            var maxEndDateAllowedToCreateInvoice = EbClock.GetBrasiliaToday();

            foreach (var plannedInstallment in plannedInstallments)
            {
                var endDateAllowedToCreateInvoice = EbClock.GetBrasiliaToday().GetEndOfNextYear();

                if (plannedInstallment.EnrollmentId != null && !ValidateInvoiceTransfers())
                {
                    var year = plannedInstallment.AcademicClassYear ?? EbClock.CurrentYear;
                    endDateAllowedToCreateInvoice = new DateTime(year + 1, 03, 01).AddMilliseconds(-1);
                }
                else
                {
                    endDateAllowedToCreateInvoice = DueDate;
                }

                maxEndDateAllowedToCreateInvoice = maxEndDateAllowedToCreateInvoice < endDateAllowedToCreateInvoice
                    ? endDateAllowedToCreateInvoice
                    : maxEndDateAllowedToCreateInvoice;

                if (DueDate > maxEndDateAllowedToCreateInvoice &&
                    plannedInstallment.EbpayPlan is CalculatorStrategyEnum.ZeroDefault or null)
                    throw new BusinessException(ExceptionConsts.Invoice.InvalidDueDate)
                        .WithData("date", maxEndDateAllowedToCreateInvoice.ToString(CultureInfo.CurrentUICulture));
            }

            PlannedInstallments = plannedInstallments;
        }

        private bool ValidateInvoiceTransfers()
        {
            return  ValidTransactions.Any(x =>
                    Equals(x.TransactionType, TransactionType.Transfer) ||
                    Equals(x.TransactionType, TransactionType.Retention));
        }

        private EarlyPaymentDiscount GetPreviousEarlyPaymentDiscount()
        {
            if (!HasEarlyPaymentDiscounts)
                return null;

            var today = EbClock.GetBrasiliaToday();
            EarlyPaymentDiscount earlyPaymentDiscountApplicable = null;

            var applicables = EarlyPaymentDiscounts.Where(x => x.LimitDiscountDate.Date < today);

            if (applicables.Any())
                earlyPaymentDiscountApplicable = applicables.OrderBy(x => x.Days).First();

            return earlyPaymentDiscountApplicable;
        }

        /// <summary>
        /// Busca o valor do desconto encontrado à ser aplicado em porcentagem.
        /// </summary>
        /// <returns>Porcentagem do desconto à ser aplicado.</returns>
        private decimal GetEffectiveDiscountPercent()
        {
            var applicable = GetEarlyPaymentDiscountApplicable();

            if (applicable == null)
                return 0;

            var result = PaymentDate != null
                ? applicable.GetEffectiveDiscountPercent(DueDate, TotalItemsCents, PaymentDate.Value.Date)
                : applicable.GetEffectiveDiscountPercent(DueDate, TotalItemsCents);

            return result;
        }

        private EarlyPaymentDiscount GetEarlyPaymentDiscountApplicable()
        {
            if (!HasEarlyPaymentDiscounts)
                return null;

            var today = EbClock.GetBrasiliaToday();
            EarlyPaymentDiscount earlyPaymentDiscountApplicable = null;

            var applicables = EarlyPaymentDiscounts.Where(x => x.LimitDiscountDate >= today);

            if (applicables.Any())
                earlyPaymentDiscountApplicable = applicables.OrderBy(x => x.Days).Last();

            return earlyPaymentDiscountApplicable;
        }

        private long GetPreviousDiscountCents()
        {
            var applicable = GetPreviousEarlyPaymentDiscount();

            if (applicable == null)
                return 0;

            var result = applicable.GetDiscountInCents(TotalItemsCents);

            return result;
        }

        private void SetPaymentGatewayInvoices(List<PaymentGatewayInvoice> paymentGatewayInvoices)
        {
            if (paymentGatewayInvoices == null)
                return;

            PaymentGatewayInvoices.Clear();

            foreach (var item in paymentGatewayInvoices)
            {
                AddPaymentGatewayInvoice(item);
            }
        }

        private void AddPaymentGatewayInvoice(PaymentGatewayInvoice paymentGatewayInvoice)
        {
            if (paymentGatewayInvoice == null || PaymentGatewayInvoices.Contains(paymentGatewayInvoice))
                return;

            PaymentGatewayInvoices.Add(paymentGatewayInvoice);
        }

        private void SetEarlyPaymentDiscounts(List<EarlyPaymentDiscount> earlyPaymentDiscounts)
        {
            if (earlyPaymentDiscounts == null)
            {
                EarlyPaymentDiscounts = new List<EarlyPaymentDiscount>();
                return;
            }

            if (earlyPaymentDiscounts.Any(earlyPaymentDiscount =>
                    earlyPaymentDiscounts.Count(x => x.ValueEquals(earlyPaymentDiscount)) > 1))
            {
                throw new BusinessException(ExceptionConsts.Invoice.DiscountDuplicate);
            }

            if (earlyPaymentDiscounts.Count != earlyPaymentDiscounts.DistinctBy(x => x.Days).Count())
            {
                throw new BusinessException(ExceptionConsts.Invoice.DiscountSameDate);
            }

            if (earlyPaymentDiscounts.Count > 3)
                throw new BusinessException(ExceptionConsts.Invoice.ExceededLimitOf3EarlyPaymentDiscount);

            // Se o valor do desconto for maior do que o total da fatura, lançar exception
            if (earlyPaymentDiscounts.Any(x => x.ValueCents != null && x.ValueCents.Value > TotalItemsCents))
            {
                throw new BusinessException(ExceptionConsts.Invoice.DiscountValueCentsGreaterThanTotalCents);
            }

            // Se data limite do desconto for maior que a data limite de pagamento, lançar exception
            var limitDueDate = DueDate;
            while (EbClock.IsHolidayOrWeekend(limitDueDate))
            {
                limitDueDate = limitDueDate.AddDays(1);
            }

            if (earlyPaymentDiscounts.Any(x => x.LimitDiscountDate.Date > limitDueDate))
            {
                throw new BusinessException(ExceptionConsts.Invoice.DiscountLimitDateGreaterThanLimitDueDate);
            }

            /// Não permitir algum desconto com dia e valor superior à posição anterior.
            /// Ex: Até a data de vencimento dar desconto de R$100,00, 5 dias antes do vencimento dar desconto de R$200,00
            var orderedEnumerable = earlyPaymentDiscounts.OrderBy(x => x.Days).ToList();
            if (orderedEnumerable.Count > 1)
            {
                for (var i = 1; i < orderedEnumerable.Count; i++)
                {
                    var invalid = orderedEnumerable[i].ValueCents <= orderedEnumerable[i - 1].ValueCents;
                    if (invalid)
                        throw new BusinessException(ExceptionConsts.Invoice.EarlyPaymentDiscountValueNotGreaterOrEqual);
                }
            }

            EarlyPaymentDiscounts = earlyPaymentDiscounts;
        }

        private void SetPaymentFine(PaymentFine paymentFine)
        {
            if (paymentFine != null)
            {
                ValidatePaymentFineDailyInterest(paymentFine.DailyInterest);

                ValidatePaymentFineOverdueFine(paymentFine.OverdueFine);
            }

            PaymentFine = paymentFine;
        }

        public void SetIpcaFine(IpcaFine ipcaFine)
        {
            if (ipcaFine != null)
                IpcaFine = ipcaFine;
        }

        public long GetIpcaFineTotalCents()
        {
            if (IpcaFine != null && IpcaFine.IpcaTotalCents > 0)
                return IpcaFine.IpcaTotalCents;

            return 0;
        }

        private void ValidatePaymentFineDailyInterest(decimal? dailyInterest)
        {
            if (dailyInterest is null or 0) return;
            if (!dailyInterest.Value.HasNdecimalPlaces(4))
                throw new BusinessException(ExceptionConsts.Invoice.InvalidDailyInterestDecimalPlaces)
                    .WithData("valor", dailyInterest.ToString())
                    .WithData("valorPorcentagem", (dailyInterest * 100m).ToString().TrimEnd('0'));

            if (dailyInterest * TotalItemsCents / 30 < 1)
                throw new BusinessException(ExceptionConsts.Invoice
                    .NotPossibleCloseInvoiceWithDailyInterestLessThan1Cent);
        }

        private static void ValidatePaymentFineOverdueFine(decimal? overdueFine)
        {
            if (overdueFine is null or 0) return;
            if (!overdueFine.Value.HasNdecimalPlaces(4))
                throw new BusinessException(ExceptionConsts.Invoice.InvalidOverdueFineDecimalPlaces)
                    .WithData("valor", overdueFine.ToString())
                    .WithData("valorPorcentagem", (overdueFine * 100).ToString().TrimEnd('0'));
        }

        internal void SetPayer(Payer payer)
        {
            if (payer == null)
                throw new AbpException("Payer cannot be null");

            if (payer.Document == null)
                throw new AbpException("Payer document cannot be null");

            if (string.IsNullOrEmpty(payer.Name))
                throw new AbpException("Payer name cannot be null");

            Payer = payer;
        }

        public void AddTransaction(Transaction transaction)
        {
            Transactions.Add(transaction);
            CalculateBalance();
        }

        public void AddTransaction(List<Transaction> transactions)
        {
            Transactions.AddRange(transactions);
            CalculateBalance();
        }

        public void CalculateBalance()
        {
            var invoiceBalance = ValidTransactions
                .Where(x => x.TransactionSide == TransactionSide.School)
                .Sum(x => x.ValueCents);

            if (invoiceBalance.IsBetween(-10, 10))
                invoiceBalance = 0;

            Balance = invoiceBalance;

            var educbankBalance = ValidTransactions
                .Where(x => x.TransactionSide == TransactionSide.Educbank)
                .Sum(x => x.ValueCents);

            if (educbankBalance.IsBetween(-10, 10))
                educbankBalance = 0;

            EducbankBalance = educbankBalance;
        }

        private List<Transaction> GetValidTransactions()
        {
            return Transactions.Where(x => !x.IsCanceled).ToList();
        }

        public void SetTransferBaseFields()
        {
            if (ValidTransactions.Any(x =>
                    Equals(x.TransactionType, TransactionType.Transfer) ||
                    Equals(x.TransactionType, TransactionType.Retention)))
                return;

            long discountValueCents = 0;

            if (EarlyPaymentDiscounts != null && EarlyPaymentDiscounts.Any())
            {
                discountValueCents =
                    EarlyPaymentDiscounts.OrderBy(x => x.Days).Last().GetDiscountInCents(TotalItemsCents);
            }

            ValueToCalculateTransfer = TotalItemsCents - TotalFixedDiscountCents - discountValueCents;
            DateToCalculateTransfer = DueDate;
            EffectiveDateToCalculateTransfer = EbClock.GetNextBusinessDay(DateToCalculateTransfer);
        }

        public void SetImportByCM(long valueBaseCents)
        {
            ValueToCalculateTransfer = valueBaseCents;
        }

        internal void AddClosedEvent(Guid eventId, Guid paymentGatewayInvoiceId, bool createAsync, string guardianNames, string eventType)
        {
            AddDistributedEvent(
                new InvoiceClosedEto
                {
                    InvoiceId = Id,
                    TenantId = TenantId,
                    PaymentGatewayInvoiceId = paymentGatewayInvoiceId,
                    CreatePaymentGatewayInvoiceAsync = createAsync,
                    CreationTime = EbClock.Now,
                    EventId = eventId,
                    DueDate = DueDate,
                    EventBalance = CalculateEventBalance(eventId),
                    SchoolId = SchoolId,
                    Code = Code,
                    StudentName = string.Join(',', PlannedInstallments.Select(x=> x.StudentName)),
                    Class = string.Join(',', PlannedInstallments.Select(x=> x.AcademicClassName)),
                    InvoiceBaseValue = ValueToCalculateTransfer,
                    InvoiceBaseDueDate = DateToCalculateTransfer,
                    GuardianName = guardianNames,
                    InvoiceCompetence = ReferenceDate,
                    EventType = eventType
                }
            );
        }

        internal void AddUpdatedEvent(Guid eventId, string guardianNames, string eventType)
        {
            AddDistributedEvent(
                new InvoiceUpdatedEto
                {
                    InvoiceId = Id,
                    TenantId = TenantId,
                    CreationTime = EbClock.Now,
                    EventId = eventId,
                    DueDate = DueDate,
                    EventBalance = CalculateEventBalance(eventId),
                    SchoolId = SchoolId,
                    Code = Code,
                    StudentName = string.Join(',', PlannedInstallments.Select(x=> x.StudentName)),
                    Class = string.Join(',', PlannedInstallments.Select(x=> x.AcademicClassName)),
                    InvoiceBaseValue = ValueToCalculateTransfer,
                    InvoiceBaseDueDate = DateToCalculateTransfer,
                    GuardianName = guardianNames,
                    InvoiceCompetence = ReferenceDate,
                    EventType = eventType
                }
            );
        }

        internal void AddCanceledEvent(Guid eventId, string guardianNames, string eventType)
        {
            AddDistributedEvent(
                new InvoiceCanceledEto
                {
                    InvoiceId = Id,
                    TenantId = TenantId,
                    CreationTime = EbClock.Now,
                    EventId = eventId,
                    DueDate = DueDate,
                    EventBalance = CalculateEventBalance(eventId),
                    SchoolId = SchoolId,
                    Code = Code,
                    StudentName = string.Join(',', PlannedInstallments.Select(x=> x.StudentName)),
                    Class = string.Join(',', PlannedInstallments.Select(x=> x.AcademicClassName)),
                    InvoiceBaseValue = ValueToCalculateTransfer,
                    InvoiceBaseDueDate = DateToCalculateTransfer,
                    GuardianName = guardianNames,
                    InvoiceCompetence = ReferenceDate,
                    EventType = eventType
                }
            );
        }

        internal void AddPaidEvent(Guid eventId, string guardianNames, string eventType)
        {
            AddDistributedEvent(
                new InvoicePaidEto
                {
                    InvoiceId = Id,
                    TenantId = TenantId,
                    CreationTime = EbClock.Now,
                    EventId = eventId,
                    DueDate = DueDate,
                    EventBalance = CalculateEventBalance(eventId),
                    SchoolId = SchoolId,
                    Code = Code,
                    StudentName = string.Join(',', PlannedInstallments.Select(x=> x.StudentName)),
                    Class = string.Join(',', PlannedInstallments.Select(x=> x.AcademicClassName)),
                    InvoiceBaseValue = ValueToCalculateTransfer,
                    InvoiceBaseDueDate = DateToCalculateTransfer,
                    GuardianName = guardianNames,
                    InvoiceCompetence = ReferenceDate,
                    EventType = eventType
                }
            );
        }

        internal void AddPaidAtSchoolEvent(Guid eventId, Guid? oldPaymentGatewayInvoiceId, string guardianNames, string eventType)
        {
            AddDistributedEvent(
                new InvoicePaidAtSchoolEto
                {
                    InvoiceId = Id,
                    TenantId = TenantId,
                    OldPaymentGatewayInvoiceId = oldPaymentGatewayInvoiceId,
                    CreationTime = EbClock.Now,
                    EventId = eventId,
                    DueDate = DueDate,
                    EventBalance = CalculateEventBalance(eventId),
                    SchoolId = SchoolId,
                    Code = Code,
                    StudentName = string.Join(',', PlannedInstallments.Select(x=> x.StudentName)),
                    Class = string.Join(',', PlannedInstallments.Select(x=> x.AcademicClassName)),
                    InvoiceBaseValue = ValueToCalculateTransfer,
                    InvoiceBaseDueDate = DateToCalculateTransfer,
                    GuardianName = guardianNames,
                    InvoiceCompetence = ReferenceDate,
                    EventType = eventType
                }
            );
        }

        internal void AddDuplicatedPaymentEvent(Guid eventId, string guardianNames, string eventType)
        {
            AddDistributedEvent(
                new DuplicatedPaymentEto
                {
                    InvoiceId = Id,
                    TenantId = TenantId,
                    CreationTime = EbClock.Now,
                    EventId = eventId,
                    DueDate = DueDate,
                    EventBalance = CalculateEventBalance(eventId),
                    SchoolId = SchoolId,
                    Code = Code,
                    StudentName = string.Join(',', PlannedInstallments.Select(x=> x.StudentName)),
                    Class = string.Join(',', PlannedInstallments.Select(x=> x.AcademicClassName)),
                    InvoiceBaseValue = ValueToCalculateTransfer,
                    InvoiceBaseDueDate = DateToCalculateTransfer,
                    GuardianName = guardianNames,
                    InvoiceCompetence = ReferenceDate,
                    EventType = eventType
                }
            );
        }

        internal void AddReEnrollmentEvent(Guid eventId, string guardianNames, string eventType)
        {
            AddDistributedEvent(
                new InvoiceReEnrollmentEto
                {
                    InvoiceId = Id,
                    TenantId = TenantId,
                    CreationTime = EbClock.Now,
                    EventId = eventId,
                    DueDate = DueDate,
                    EventBalance = CalculateEventBalance(eventId),
                    SchoolId = SchoolId,
                    Code = Code,
                    StudentName = string.Join(',', PlannedInstallments.Select(x=> x.StudentName)),
                    Class = string.Join(',', PlannedInstallments.Select(x=> x.AcademicClassName)),
                    InvoiceBaseValue = ValueToCalculateTransfer,
                    InvoiceBaseDueDate = DateToCalculateTransfer,
                    GuardianName = guardianNames,
                    InvoiceCompetence = ReferenceDate,
                    EventType = eventType
                }
            );
        }

        internal void AddCancelReEnrollmentEvent(Guid eventId, string guardianNames, string eventType)
        {
            AddDistributedEvent(
                new CancelReEnrollmentEto
                {
                    InvoiceId = Id,
                    TenantId = TenantId,
                    CreationTime = EbClock.Now,
                    EventId = eventId,
                    DueDate = DueDate,
                    EventBalance = CalculateEventBalance(eventId),
                    SchoolId = SchoolId,
                    Code = Code,
                    StudentName = string.Join(',', PlannedInstallments.Select(x=> x.StudentName)),
                    Class = string.Join(',', PlannedInstallments.Select(x=> x.AcademicClassName)),
                    InvoiceBaseValue = ValueToCalculateTransfer,
                    InvoiceBaseDueDate = DateToCalculateTransfer,
                    GuardianName = guardianNames,
                    InvoiceCompetence = ReferenceDate,
                    EventType = eventType
                }
            );
        }

        internal void AddPaymentCanceledEvent(Guid eventId, string guardianNames, string eventType)
        {
            AddDistributedEvent(
                new InvoicePaymentCanceledEto
                {
                    InvoiceId = Id,
                    TenantId = TenantId,
                    CreationTime = EbClock.Now,
                    EventId = eventId,
                    DueDate = DueDate,
                    EventBalance = CalculateEventBalance(eventId),
                    SchoolId = SchoolId,
                    Code = Code,
                    StudentName = string.Join(',', PlannedInstallments.Select(x=> x.StudentName)),
                    Class = string.Join(',', PlannedInstallments.Select(x=> x.AcademicClassName)),
                    InvoiceBaseValue = ValueToCalculateTransfer,
                    InvoiceBaseDueDate = DateToCalculateTransfer,
                    GuardianName = guardianNames,
                    InvoiceCompetence = ReferenceDate,
                    EventType = eventType
                }
            );
        }

        internal void AddEducbankTaxEvent(Guid eventId, string guardianNames, string eventType)
        {
            AddDistributedEvent(
                new AddedEducbankTaxEto
                {
                    InvoiceId = Id,
                    TenantId = TenantId,
                    CreationTime = EbClock.Now,
                    EventId = eventId,
                    DueDate = DueDate,
                    EventBalance = CalculateEventBalance(eventId),
                    SchoolId = SchoolId,
                    Code = Code,
                    StudentName = string.Join(',', PlannedInstallments.Select(x=> x.StudentName)),
                    Class = string.Join(',', PlannedInstallments.Select(x=> x.AcademicClassName)),
                    InvoiceBaseValue = ValueToCalculateTransfer,
                    InvoiceBaseDueDate = DateToCalculateTransfer,
                    GuardianName = guardianNames,
                    InvoiceCompetence = ReferenceDate,
                    EventType = eventType
                }
            );
        }

        public void LiquidateByCancellation(DateTime liquidationDate, long valueCents)
        {
            if(Liquidation?.Date != null)
            {
                return;
            }
            Liquidation = new Liquidation
            {
                Date = liquidationDate,
                Reason = LiquidationReason.Cancellation,
                ValueCents = valueCents
            };
            AddDistributedEvent(new LiquidatedByCancellationEto
            {
                InvoiceId = Id,
                CancellationDate = PaymentGatewayInvoice.CancelationDate.Value,
                CancellationReason = PaymentGatewayInvoice.CancelationReason.ToString(),
                LiquidationDate = liquidationDate
            });
        }

        public void LiquidateByCreditCardPayment(long valueCents)
        {
            if(Liquidation?.Date != null)
            {
                return;
            }
            Liquidation = new Liquidation
            {
                Date = PaymentGatewayInvoice.PaymentDate.Value,
                Reason = LiquidationReason.CreditCardPayment,
                ValueCents = valueCents
            };

            AddDistributedEvent(new LiquidatedByCreditCardPaymentEto
            {
                InvoiceId = Id,
                NossoNumero = PaymentGatewayInvoice.BankSlip.OurNumber,
                LiquidationDate = PaymentGatewayInvoice.PaymentDate.Value,
                PaymentDate = PaymentGatewayInvoice.PaymentDate.Value
            });
        }

        public void LiquidateByPaymentAtSchool(DateTime liquidationDate, long valueCents)
        {
            if(Liquidation?.Date != null)
            {
                return;
            }
            Liquidation = new Liquidation
            {
                Date = liquidationDate,
                Reason = LiquidationReason.SchoolPayment,
                ValueCents = valueCents
            };

            AddDistributedEvent(new LiquidatedByPaymentAtSchoolEto
            {
                InvoiceId = Id,
                LiquidationDate = liquidationDate,
                PaymentDate = PaymentGatewayInvoice.PaymentDate.Value
            });
        }

        public void LiquidateByPayment(DateTime liquidationDate, long valueCents)
        {
            if(Liquidation?.Date != null)
            {
                return;
            }
            Liquidation = new Liquidation
            {
                Date = liquidationDate,
                Reason = LiquidationReason.Payment,
                ValueCents = valueCents
            };

            AddDistributedEvent(new LiquidatedByPaymentEto
            {
                InvoiceId = Id,
                NossoNumero = PaymentGatewayInvoice.BankSlip.OurNumber,
                LiquidationDate = liquidationDate,
                PaymentDate = PaymentGatewayInvoice.PaymentDate.Value,
                TotalPaidCents = valueCents,
                PaidMethod = PaymentGatewayInvoice.PaidMethod.Value
            });
        }

        public void LiquidateByReEnrollment(DateTime liquidationDate, long valueCents)
        {
            if(Liquidation?.Date != null)
            {
                return;
            }
            Liquidation = new Liquidation
            {
                Date = liquidationDate,
                Reason = LiquidationReason.ReEnrollment,
                ValueCents = valueCents
            };

            AddDistributedEvent(new LiquidatedByReEnrollmentEto
            {
                InvoiceId = Id,
                NossoNumero = PaymentGatewayInvoice.BankSlip.OurNumber,
                LiquidationDate = liquidationDate,
                ReEnrollmentDate = GetReEnrollmentDate()
            });
        }

        internal void AddLiquidationDuplicatedEvent(DateTime liquidationDate)
        {
            AddDistributedEvent(new LiquidatedByDuplicatedPaymentEto
            {
                InvoiceId = Id,
                NossoNumero = PaymentGatewayInvoice.BankSlip.OurNumber,
                LiquidationDate = liquidationDate,
                PaymentDate = PaymentGatewayInvoice.PaymentDate.Value,
                TotalPaidCents = PaymentGatewayInvoice.TotalPaidCents.Value
            });
        }

        internal long CalculateEventBalance(Guid eventId)
        {
            return ValidTransactions.Where(x => x.InvoiceEventId == eventId && x.TransactionSide == TransactionSide.School)
                .Sum(x => x.ValueCents) * -1;


        }

        private DateTime GetReEnrollmentDate()
        {
            var transaction = ValidTransactions
                .First(x=> x.TransactionType == TransactionType.NewInvoiceByGuardianDocument || x.TransactionType == TransactionType.ReEnrollment || x.TransactionType == TransactionType.NewEnrollmentByGuardianDocument);

            return transaction.OccurrenceDate;
        }
    }

    public class InvoiceLog : ValueObject
    {
        /// <summary>
        /// Log description to be translated
        /// </summary>
        /// <value></value>
        public string Description { get; set; }

        /// <summary>
        /// If true the log can be showed to final user
        /// </summary>
        /// <value></value>
        public bool IsVisibleToClient { get; set; }

        /// <summary>
        /// Log payload
        /// </summary>
        /// <value></value>
        public string Payload { get; set; }

        /// <summary>
        /// Log creation time
        /// </summary>
        /// <value></value>
        public DateTime CreationTime { get; private set; }

        public InvoiceLog(object payload, string description = null, bool isVisibleToClient = true)
        {
            Payload = JsonSerializer.Serialize(payload);
            IsVisibleToClient = isVisibleToClient;
            CreationTime = EbClock.Now;

            var command = payload as IInvoiceCommand;
            Description = description ?? command?.Description;
        }

        protected override IEnumerable<object> GetAtomicValues()
        {
            yield return Payload;
            yield return IsVisibleToClient;
            yield return Description;
            yield return CreationTime;
        }
    }

    public class CalculatedCharges
    {
        public DateTime CalculationTime { get; set; }
        public long MonetaryAdjustment { get; set; }
        public long Fine { get; set; }
        public long Interest { get; set; }
    }

    public sealed class InvoiceEvents : CreationAuditedEntity<Guid>
    {
        public InvoiceEventType EventType { get; set; }

        public InvoiceEvents(Guid id, InvoiceEventType eventType, DateTime? creationTime) : base(id)
        {
            EventType = eventType;
            CreationTime = creationTime ?? EbClock.Now;
        }
    }

    public sealed class Liquidation
    {
        public DateTime Date { get; set; }
        public long ValueCents { get; set; }
        public LiquidationReason Reason { get; set; }
    }
}
