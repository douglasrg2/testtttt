using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Educbank.Core.Invoices;
using Educbank.Core.Settings;
using Educbank.Core.ValueObjects;
using Educbank.Timing;
using Educbank.ValueObjetcs;
using Volo.Abp;
using Volo.Abp.Data;
using Volo.Abp.Settings;

namespace Educbank.Core.CalculatorStrategies
{
    public class ZeroDefaultCalculatorStrategy : CalculatorStrategy
    {
        private const string InternalMethodName = "zerodefault";
        private const string EducbankTaxPropertyName = "educbanktax";
        private readonly ISettingProvider _settingsProvider;
        private readonly InvoiceManager _invoiceManager;

        //key: tipo de retenção, value: transação de pagamento de volta relacionado à retenção
        private static readonly IDictionary<string, TransactionType> MapRetentionTransactionTypes =
            new Dictionary<string, TransactionType>
            {
                { TransactionType.ReEnrollment, TransactionType.ReEnrollmentPayback },
                {
                    TransactionType.NewEnrollmentByGuardianDocument,
                    TransactionType.NewEnrollmentByGuardianDocumentPayback
                },
                { TransactionType.NewInvoiceByGuardianDocument, TransactionType.NewInvoiceByGuardianDocumentPayback },
            };

        public ZeroDefaultCalculatorStrategy(ISettingProvider setting, InvoiceManager invoiceManager)
        {
            _settingsProvider = setting;
            _invoiceManager = invoiceManager;
        }

        public override string MethodName => InternalMethodName;

        public override async Task AddInvoiceCreatedTransactions(Invoice invoice, DateTime eventOccurrenceDate, Guid eventId)
        {
            if (invoice.ValidTransactions.Any(x => Equals(x.TransactionType, TransactionType.SchoolItem)))
                return;

            var shouldWriteTransaction = await ShouldWriteTransaction(InvoiceEventType.Created, invoice);
            if (!shouldWriteTransaction)
                return;

            var items = invoice.PlannedInstallments;

            foreach (var item in items)
            {
                var transaction = new Transaction(GuidGenerator.Create(), MethodName, item.ValueCents * -1,
                    TransactionType.SchoolItem, eventOccurrenceDate, TransactionSide.School, eventId);

                transaction.SetProperty("ItemName", item.ItemName);
                transaction.SetProperty("ValueInReais", (double)item.ValueCents / 100);

                invoice.AddTransaction(transaction);
            }

            var fixedDiscountCents = invoice.TotalFixedDiscountCents;

            if (fixedDiscountCents > 0)
            {
                var transaction = new Transaction(GuidGenerator.Create(), MethodName, fixedDiscountCents,
                    TransactionType.FixedDiscount, eventOccurrenceDate, TransactionSide.School, eventId);
                transaction.SetProperty("DiscountInReais", (double)fixedDiscountCents / 100);

                invoice.AddTransaction(transaction);
            }

            if (invoice.HasEarlyPaymentDiscounts)
            {
                var earlyPaymentDiscount = invoice.EarlyPaymentDiscounts.OrderBy(x => x.Days).Last();
                var earlyPaymentDiscountCents = earlyPaymentDiscount.GetDiscountInCents(invoice.TotalItemsCents);

                if (earlyPaymentDiscountCents > 0)
                {
                    var transaction = new Transaction(GuidGenerator.Create(), MethodName, earlyPaymentDiscountCents,
                        TransactionType.EarlyPaymentDiscount, eventOccurrenceDate, TransactionSide.School, eventId);
                    transaction.SetProperty("DiscountInReais", (double)earlyPaymentDiscountCents / 100);
                    transaction.SetProperty("DaysOfDiscount", earlyPaymentDiscount.Days);

                    invoice.AddTransaction(transaction);
                }
            }

            var educbankTax = await GetEducbankTax(invoice.CompanyCnpj);

            WriteEducbankTaxTransaction(invoice, null, invoice.ValueToCalculateTransfer, eventOccurrenceDate,
                educbankTax, eventId);
        }

        public override async Task AddEducbankTaxTransactions(Invoice invoice, Guid eventId)
        {
            if (invoice.Transactions.Count > 0)
                invoice.Transactions.RemoveAll(r =>
                    r.TransactionType.Equals(TransactionType.EducbankTax) && r.ValueCents <= 0 && !r.IsCanceled);

            var educbankTaxTransactions = invoice.Transactions.FindAll(f =>
                f.TransactionType.Equals(TransactionType.EducbankTax) && !f.IsCanceled);

            var educbankTaxSchoolItems = invoice.Transactions.FindAll(f =>
                f.TransactionType.Equals(TransactionType.SchoolItem) && !f.IsCanceled);

            var educbankTax = await GetEducbankTax(invoice.CompanyCnpj);
            if (educbankTaxTransactions.Count == 0 && educbankTaxSchoolItems.Count > 0 && educbankTax > 0)
            {
                WriteEducbankTaxTransaction(invoice, null, invoice.ValueToCalculateTransfer, EbClock.Now, educbankTax, eventId);
            }
            else
            {
                throw new BusinessException(ExceptionConsts.ZeroDefaultStrategy.InvalidInvoiceEducbankTax).WithData(
                    "code", invoice.Code);
            }
        }

        public override async Task AddInvoicePaymentTransactions(Invoice invoice, DateTime eventOccurrenceDate, Guid eventId)
        {
            var shouldWriteTransaction = await ShouldWriteTransaction(InvoiceEventType.Paid, invoice);
            if (!shouldWriteTransaction)
                return;

            await WriteIpcaChargesTransaction(invoice, eventOccurrenceDate, eventId);

            switch (invoice.PaidMethod)
            {
                case null:
                    return;
                case InvoicePaymentMethod.School:
                    await WriteSchoolPayment(invoice, eventOccurrenceDate, eventId);
                    break;
                default:
                    await WriteBankSlipPayment(invoice, eventOccurrenceDate, eventId);
                    break;
            }

            WriteBankFeeTransaction(invoice, eventOccurrenceDate, eventId);
        }

        public override async Task AddInvoiceReverseCancellationTransactions(Invoice invoice, Guid eventId)
        {
            var shouldWriteTransaction = await ShouldWriteTransaction(InvoiceEventType.Paid, invoice);
            if (!shouldWriteTransaction)
                return;

            WriteReverseCancellation(invoice, EbClock.Now, eventId);
        }

        public override async Task AddInvoiceReversePaymentAtSchoolTransactions(Invoice invoice, Guid eventId)
        {
            var shouldWriteTransaction = await ShouldWriteTransaction(InvoiceEventType.Paid, invoice);
            if (!shouldWriteTransaction)
                return;

            WriteReversePaymentAtSchool(invoice, EbClock.Now, eventId);
        }

        public override async Task AddInvoicePaymentDuplicatedTransactions(Invoice invoice, DateTime eventOccurrenceDate,
            long duplicatedTotalPaid, Guid eventId)
        {
            var shouldWriteTransaction = await ShouldWriteTransaction(InvoiceEventType.Paid, invoice);
            if (!shouldWriteTransaction)
                return;

            await WriteIpcaChargesTransaction(invoice, eventOccurrenceDate, eventId);

            WritePaymentDuplicated(invoice, eventOccurrenceDate, duplicatedTotalPaid, eventId);
        }

        public override void AddTransferTransactions(Invoice invoice, DateTime eventOccurrenceDate, DateTime transferDate, Guid eventId)
        {
            void SetProperties(Transaction transaction)
            {
                var value = transaction.ValueCents > 0 ? transaction.ValueCents : transaction.ValueCents * -1;
                transaction.SetProperty("ValueInReais", (double)value / 100);
                transaction.SetProperty("TransferRealDate", transferDate);
            }

            if (invoice.Balance == 0)
                return;

            var transferValue = invoice.Balance * -1;
            var transactionType = transferValue > 0 ? TransactionType.Transfer : TransactionType.Retention;
            var transaction = new Transaction(GuidGenerator.Create(), MethodName, transferValue,
                transactionType, eventOccurrenceDate, TransactionSide.School, eventId);

            SetProperties(transaction);

            invoice.AddTransaction(transaction);

            transaction = new Transaction(GuidGenerator.Create(), MethodName, transferValue * -1,
                transactionType, eventOccurrenceDate, TransactionSide.Educbank, eventId);

            SetProperties(transaction);

            invoice.AddTransaction(transaction);
        }

        public override async Task AddInvoiceCanceledTransactions(Invoice invoice, DateTime eventOccurrenceDate, Guid eventId)
        {
            var shouldWriteTransaction = await ShouldWriteTransaction(InvoiceEventType.Created, invoice);
            if (!shouldWriteTransaction)
                return;

            var hasReEnrollmentRetention = CheckIfHasRetentionByEnrollment(invoice);
            if (hasReEnrollmentRetention)
            {
                return;
            }

            await WriteIpcaChargesTransaction(invoice, eventOccurrenceDate, eventId);

            var hasTransfer = invoice.ValidTransactions.Any(x =>
                Equals(x.TransactionType, TransactionType.Transfer) ||
                Equals(x.TransactionType, TransactionType.Retention));
            long valueCents;

            if (!hasTransfer)
            {
                valueCents = invoice.Balance * -1;
                var educbankBalance = invoice.EducbankBalance * -1;


                if (invoice.Transactions.Count == 0 && valueCents == 0)
                    return;

                var transactionId = GuidGenerator.Create();
                var transaction = new Transaction(transactionId, MethodName, valueCents,
                    TransactionType.CancellationBeforeTransfer, eventOccurrenceDate, TransactionSide.School, eventId);

                transaction.SetProperty("OccurrenceDate", transaction.OccurrenceDate);
                invoice.AddTransaction(transaction);

                transactionId = GuidGenerator.Create();
                transaction = new Transaction(transactionId, MethodName, educbankBalance,
                    TransactionType.CancellationBeforeTransfer, eventOccurrenceDate, TransactionSide.Educbank,eventId, false,
                    transaction.Id);

                transaction.SetProperty("OccurrenceDate", transaction.OccurrenceDate);
                invoice.AddTransaction(transaction);
            }
            else
            {
                var hasRetention = HasRetention(invoice);
                if (hasRetention)
                    return;

                valueCents = invoice.ValueToCalculateTransfer;

                var transactionId = GuidGenerator.Create();
                var transaction = new Transaction(transactionId, MethodName, valueCents,
                    TransactionType.CancellationAfterTransfer, eventOccurrenceDate, TransactionSide.School, eventId);

                transaction.SetProperty("OccurrenceDate", transaction.OccurrenceDate);
                invoice.AddTransaction(transaction);

                await WriteCancellationChargesTransaction(invoice, eventOccurrenceDate, transactionId, eventId);
            }
        }

        public override async Task AddTaxAdjustmentTransactions(Invoice invoice, DateTime eventOccurrenceDate,
            decimal newEducbankTax, Guid eventId)
        {
            void SetProperties(decimal newTax, Transaction transaction, decimal oldTax)
            {
                transaction.SetProperty("OldEducbankTax", oldTax);
                transaction.SetProperty("NewEducbankTax", newTax);
                transaction.SetProperty("OccurrenceDate", transaction.OccurrenceDate);
            }

            var taxTransactions = invoice.ValidTransactions.Where(x =>
                (x.TransactionType == TransactionType.EducbankTax.Value ||
                 x.TransactionType == TransactionType.TaxAdjustment.Value) &&
                x.TransactionSide == TransactionSide.School);

            var itemsTransactions = invoice.ValidTransactions.Where(x =>
                x.TransactionType == TransactionType.SchoolItem.Value ||
                x.TransactionType == TransactionType.EarlyPaymentDiscount.Value);

            var itemsTransactionsValueSum = itemsTransactions.Sum(x => x.ValueCents);
            var newTaxValueCents = Convert.ToInt64((itemsTransactionsValueSum * newEducbankTax) * -1);
            var transactionValueCents = newTaxValueCents;

            foreach (var taxTransaction in taxTransactions)
            {
                transactionValueCents -= taxTransaction.ValueCents;
            }

            if (transactionValueCents == 0)
                return;

            var transaction = new Transaction(GuidGenerator.Create(), MethodName, transactionValueCents,
                TransactionType.TaxAdjustment, eventOccurrenceDate, TransactionSide.School, eventId);

            var oldEducbankTax = await GetEducbankTax(invoice.CompanyCnpj);
            SetProperties(newEducbankTax, transaction, oldEducbankTax);

            invoice.AddTransaction(transaction);

            transaction = new Transaction(GuidGenerator.Create(), MethodName, transactionValueCents * -1,
                TransactionType.TaxAdjustment, eventOccurrenceDate, TransactionSide.Educbank, eventId);

            SetProperties(newEducbankTax, transaction, oldEducbankTax);
            invoice.AddTransaction(transaction);
        }

        public override async Task AddInvoiceUpdatedTransactions(Invoice invoice, DateTime eventOccurrenceDate, Guid eventId)
        {
            var shouldWriteTransaction = await ShouldWriteTransaction(InvoiceEventType.Created, invoice);
            if (!shouldWriteTransaction)
                return;

            if (invoice.ValidTransactions.Any(x =>
                    Equals(x.TransactionType, TransactionType.Transfer) ||
                    Equals(x.TransactionType, TransactionType.Retention)))
                return;

            var invoiceTransactions = invoice.ValidTransactions;

            foreach (var transaction in invoiceTransactions)
            {
                transaction.IsCanceled = true;
            }

            await AddInvoiceCreatedTransactions(invoice, eventOccurrenceDate, eventId);
        }

        public override async Task AddInvoiceReEnrollmentTransactions(Invoice invoice, DateTime eventOccurrenceDate, Guid eventId)
        {
            if (invoice.IsPaid)
                return;

            var shouldWriteTransaction = await ShouldWriteTransaction(InvoiceEventType.ReEnrollment, invoice) &&
                                         CanWriteRetentionByContractWithDebtorTransaction(invoice);
            if (!shouldWriteTransaction)
                return;

            var valueCents = invoice.ValueToCalculateTransfer;

            var transaction = new Transaction(GuidGenerator.Create(), MethodName, valueCents,
                TransactionType.ReEnrollment, eventOccurrenceDate, TransactionSide.School, eventId);

            transaction.SetProperty("OccurrenceDate", transaction.OccurrenceDate);
            transaction.SetProperty("ValueInReais", (double)transaction.ValueCents / 100);
            invoice.AddTransaction(transaction);

            await WriteChargesTransaction(invoice, eventOccurrenceDate, transaction.Id, TransactionType.ReEnrollmentCharges, eventId);
        }

        public override async Task AddInvoiceNewEnrollmentByGuardianDocumentTransaction(Invoice invoice,
            DateTime eventOccurrenceDate, Guid newEnrollmentId, Guid eventId)
        {
            if (invoice.IsPaid)
                return;

            var shouldWriteTransaction = await ShouldWriteTransaction(InvoiceEventType.ReEnrollment, invoice) &&
                                         CanWriteRetentionByContractWithDebtorTransaction(invoice);
            if (!shouldWriteTransaction)
                return;

            var valueCents = invoice.ValueToCalculateTransfer;

            var transaction = new Transaction(GuidGenerator.Create(), MethodName, valueCents,
                TransactionType.NewEnrollmentByGuardianDocument, eventOccurrenceDate, TransactionSide.School, eventId);

            transaction.SetProperty("OccurrenceDate", transaction.OccurrenceDate);
            transaction.SetProperty("ValueInReais", (double)transaction.ValueCents / 100);
            transaction.SetProperty("NewEnrollmentId", newEnrollmentId.ToString());
            invoice.AddTransaction(transaction);

            await WriteChargesTransaction(invoice, eventOccurrenceDate, transaction.Id, TransactionType.NewEnrollmentByGuardianDocumentCharges, eventId);

        }

        private async Task<bool> WritePaybackTransaction(Invoice invoice, DateTime eventOccurrenceDate, TransactionType retentionType,
            Guid eventId)
        {
            var lastRetention =
                invoice.Transactions.OrderBy(x => x.CreationTime)
                    .LastOrDefault(x => Equals(x.TransactionType, retentionType) && !x.IsCanceled);

            var canWritePaybackTransaction = await CanWritePaybackTransactions(invoice, lastRetention);
            if (!canWritePaybackTransaction)
                return false;

            var totalPaidCents = invoice.PaymentGatewayInvoice.TotalPaidCents.GetValueOrDefault(0);
            var creditCardTaxCents = invoice.PaymentGatewayInvoice.CreditCardTaxCents.GetValueOrDefault(0);
            var chargeResponsible = await SettingProvider.GetOrNullAsync(EducbankSettings.ResponsibleForCreditCardTax);
            var parseResult = Enum.TryParse(chargeResponsible, out ResponsibleForCreditCardTaxType responsible);
            if (!parseResult)
                return false;

            //gets back the credit card tax, in case that the guardian is the responsible
            if (responsible == ResponsibleForCreditCardTaxType.Guardian && creditCardTaxCents != 0)
            {
                totalPaidCents -= creditCardTaxCents;
            }

            var transaction = new Transaction(GuidGenerator.Create(), MethodName, totalPaidCents * -1,
                MapRetentionTransactionTypes[retentionType], eventOccurrenceDate, TransactionSide.School, eventId, false ,lastRetention?.Id);

            transaction.SetProperty("ValueInReais", (double)transaction.ValueCents / 100);
            transaction.SetProperty("PaymentDate", invoice.PaymentDate);
            invoice.AddTransaction(transaction);

            return true;
        }

        private async Task<bool> CanWritePaybackTransactions(Invoice invoice, Transaction retentionTransaction)
        {
            if (retentionTransaction == null)
                return false;

            var shouldWriteTransaction = await ShouldWriteTransaction(InvoiceEventType.Payback, invoice);

            var hasCancellationForLastReEnrollmentTransaction = invoice.Transactions.Any(x =>
                x.TransactionType.Equals(MapRetentionTransactionTypes[retentionTransaction.TransactionType])
                && !x.IsCanceled
                && x.ReferenceId.HasValue
                && x.ReferenceId.Value.Equals(retentionTransaction.Id));

            return !hasCancellationForLastReEnrollmentTransaction && shouldWriteTransaction;
        }

        public override async Task AddInvoiceNewInvoiceByGuardianDocumentTransaction(Invoice invoice, DateTime eventOccurrenceDate,
            string invoiceCode, Guid eventId)
        {
            if (invoice.IsPaid)
                return;

            var shouldWriteTransaction = await ShouldWriteTransaction(InvoiceEventType.ReEnrollment, invoice) &&
                                         CanWriteRetentionByContractWithDebtorTransaction(invoice);
            if (!shouldWriteTransaction)
                return;

            var valueCents = invoice.ValueToCalculateTransfer;

            var transaction = new Transaction(GuidGenerator.Create(), MethodName, valueCents,
                TransactionType.NewInvoiceByGuardianDocument, eventOccurrenceDate, TransactionSide.School, eventId);

            transaction.SetProperty("OccurrenceDate", transaction.OccurrenceDate);
            transaction.SetProperty("ValueInReais", (double)transaction.ValueCents / 100);
            transaction.SetProperty("InvoiceCode", invoiceCode);
            invoice.AddTransaction(transaction);

            await WriteChargesTransaction(invoice, eventOccurrenceDate, transaction.Id, TransactionType.NewInvoiceByGuardianDocumentCharges, eventId);

        }

        public override async Task AddInvoiceCancelReEnrollmentTransactions(Invoice invoice, DateTime eventOccurrenceDate,
            Guid eventId)
        {
            var lastReEnrollmentTransaction =
                invoice.Transactions.OrderBy(x => x.CreationTime)
                    .LastOrDefault(x => Equals(x.TransactionType, TransactionType.ReEnrollment) && !x.IsCanceled);

            var canWriteCancelReEnrollmentTransactions =
                await CanWriteReEnrollmentCanceledTransactions(invoice, lastReEnrollmentTransaction);
            if (!canWriteCancelReEnrollmentTransactions)
                return;

            var transaction = new Transaction(GuidGenerator.Create(), MethodName, lastReEnrollmentTransaction.ValueCents * -1,
                TransactionType.ReEnrollmentCanceled, eventOccurrenceDate, TransactionSide.School, eventId, false, lastReEnrollmentTransaction.Id);

            transaction.SetProperty("ValueInReais", (double)transaction.ValueCents / 100);

            invoice.AddTransaction(transaction);

            var reEnrollmentChargesList = invoice.Transactions.FindAll(x =>
                x.TransactionType.Equals(TransactionType.ReEnrollmentCharges) && x.ReferenceId.HasValue &&
                x.ReferenceId.Value.Equals(lastReEnrollmentTransaction.Id));

            foreach (var transactionCharge in reEnrollmentChargesList)
            {
                var transactionToRevert = new Transaction(GuidGenerator.Create(), MethodName, transactionCharge.ValueCents * -1,
                    TransactionType.ReEnrollmentChargesCanceled, eventOccurrenceDate, transactionCharge.TransactionSide, eventId, false, transactionCharge.Id);
                transactionToRevert.SetProperty("ValueInReais", (double)transaction.ValueCents / 100);

                invoice.AddTransaction(transactionToRevert);
            }
        }

        private async Task<bool> CanWriteReEnrollmentCanceledTransactions(Invoice invoice,
            Transaction lastReEnrollmentTransaction)
        {
            if (lastReEnrollmentTransaction == null)
                return false;

            var shouldWriteTransaction = await ShouldWriteTransaction(InvoiceEventType.CancelReEnrollment, invoice);

            var hasCancellationForLastReEnrollmentTransaction = invoice.Transactions.Any(x =>
                x.TransactionType.Equals(TransactionType.ReEnrollmentCanceled)
                && !x.IsCanceled
                && x.ReferenceId.HasValue
                && x.ReferenceId.Value.Equals(lastReEnrollmentTransaction.Id));

            return !hasCancellationForLastReEnrollmentTransaction && shouldWriteTransaction;
        }

        private async Task WriteSchoolPayment(Invoice invoice, DateTime eventOccurrenceDate, Guid eventId)
        {
            var hasTransfer = invoice.ValidTransactions.Any(x =>
                Equals(x.TransactionType, TransactionType.Transfer) ||
                Equals(x.TransactionType, TransactionType.Retention));
            var transactionId = GuidGenerator.Create();
            var transactionType = TransactionType.SchoolPaymentBeforeTransfer;

            if (hasTransfer)
            {
                transactionType = TransactionType.SchoolPaymentAfterTransfer;

                var hasRetention = HasRetention(invoice);

                if (hasRetention)
                    return;
            }

            var transaction = new Transaction(transactionId, MethodName, invoice.ValueToCalculateTransfer,
                transactionType, eventOccurrenceDate, TransactionSide.School, eventId);

            transaction.SetProperty("ValueInReais", (double)transaction.ValueCents / 100);
            transaction.SetProperty("OccurrenceDate", transaction.OccurrenceDate);

            invoice.AddTransaction(transaction);

            await WritePaymentChargesTransaction(invoice, eventOccurrenceDate, transactionId,
                TransactionType.SchoolPaymentCharges, 0, TransactionSide.School, invoice.ValueToCalculateTransfer, eventId);

            await WritePaymentChargesTransaction(invoice, eventOccurrenceDate, transactionId,
                TransactionType.SchoolPaymentCharges, 0, TransactionSide.Educbank, invoice.ValueToCalculateTransfer, eventId);
        }

        //if exists retention of any kind
        private static bool HasRetention(Invoice invoice) =>
            invoice.ValidTransactions
                .Any(x =>
                    MapRetentionTransactionTypes.Any(m => Equals(m.Key, x.TransactionType.ToString())) &&
                    !x.IsCanceled);

        private async Task WriteBankSlipPayment(Invoice invoice, DateTime eventOccurrenceDate,Guid eventId)
        {
            var creditCardTaxCents = invoice.PaymentGatewayInvoice.CreditCardTaxCents.GetValueOrDefault(0);
            var chargeResponsible = await SettingProvider.GetOrNullAsync(EducbankSettings.ResponsibleForCreditCardTax);
            var parseResult = Enum.TryParse(chargeResponsible, out ResponsibleForCreditCardTaxType responsible);
            if (!parseResult)
                return;
            if (responsible != ResponsibleForCreditCardTaxType.Guardian)
                creditCardTaxCents = 0;
            var educbankExpectedValue = invoice.ValueToCalculateTransfer + creditCardTaxCents;

            var totalPaidCents = invoice.PaymentGatewayInvoice.TotalPaidCents.GetValueOrDefault(0);
            var paidCharges = invoice.PaymentGatewayInvoice.EffectiveTotalFineCents.GetValueOrDefault(0);
            var invoicePaidValue = totalPaidCents - paidCharges;
            var educbankPaymentFine = await GetEducbankPaymentFine();
            var dateToCompare = invoice.PaymentGatewayInvoice.PaymentDate ?? EbClock.Now;
            var expectedCharges = educbankPaymentFine.GetPaymentFineCents(invoice.DateToCalculateTransfer,
                invoice.ValueToCalculateTransfer, dateToCompare.Date);

            var transactionId = GuidGenerator.Create();
            var transaction = new Transaction(transactionId, MethodName, totalPaidCents,
                TransactionType.EducbankPayment, eventOccurrenceDate, TransactionSide.Educbank, eventId);

            transaction.SetProperty("BaseValueInReais", (double)transaction.ValueCents / 100);
            transaction.SetProperty("PaymentDate", invoice.PaymentDate);

            invoice.AddTransaction(transaction);

            var hasRetention = HasRetention(invoice);
            if (hasRetention)
            {
                foreach (var transactionType in MapRetentionTransactionTypes)
                {
                    var retentionTransaction = TransactionType.Parse(transactionType.Key);
                    var wasWritten = await WritePaybackTransaction(invoice, eventOccurrenceDate, retentionTransaction, eventId);

                    //Não tem como ter duas retenções, então se já escreveu, pode parar.
                    if (wasWritten)
                        return;
                }
            }

            await WritePaymentChargesTransaction(invoice, eventOccurrenceDate, transactionId,
                TransactionType.EducbankPaymentCharges, 0, TransactionSide.Educbank, invoice.ValueToCalculateTransfer, eventId);

            if (invoicePaidValue != educbankExpectedValue)
            {
                transactionId = GuidGenerator.Create();
                var diff = educbankExpectedValue - invoicePaidValue;
                transaction = new Transaction(transactionId, MethodName, diff,
                    TransactionType.PaymentDifference, eventOccurrenceDate, TransactionSide.School, eventId);

                transaction.SetProperty("ValueExpectedInReais", (double)educbankExpectedValue / 100);
                transaction.SetProperty("PaidValueInReais", (double)invoicePaidValue / 100);

                invoice.AddTransaction(transaction);
            }

            if (expectedCharges != paidCharges)
            {
                if (invoice.PaymentGatewayInvoice.PaymentDifference?.PaymentDifferenceValueCents < 0 &&
                    invoice.PaymentGatewayInvoice.PaymentFine?.OverdueFine > 0)
                {
                    return;
                }

                var chargesDiff = expectedCharges - paidCharges;

                transaction = new Transaction(GuidGenerator.Create(), MethodName, chargesDiff,
                    TransactionType.PaymentDifferenceCharges, eventOccurrenceDate, TransactionSide.School, eventId,
                    false, transaction.Id);

                transaction.SetProperty("ValueExpectedInReais", (double)expectedCharges / 100);
                transaction.SetProperty("TotalPaidFineInReais", (double)paidCharges / 100);

                invoice.AddTransaction(transaction);
            }
        }

        private void WritePaymentDuplicated(Invoice invoice, DateTime eventOccurrenceDate, long duplicatedTotalPaid, Guid eventId)
        {
            var transaction = new Transaction(GuidGenerator.Create(), MethodName, duplicatedTotalPaid * -1,
                TransactionType.DuplicatedEducbankPayment, eventOccurrenceDate, TransactionSide.School, eventId);
            transaction.SetProperty("BaseValueInReais", (double)duplicatedTotalPaid / 100);
            transaction.SetProperty("PaymentDate", eventOccurrenceDate);
            invoice.AddTransaction(transaction);

            transaction = new Transaction(GuidGenerator.Create(), MethodName, duplicatedTotalPaid,
                TransactionType.DuplicatedEducbankPayment, eventOccurrenceDate, TransactionSide.Educbank, eventId);
            transaction.SetProperty("BaseValueInReais", (double)duplicatedTotalPaid / 100);
            transaction.SetProperty("PaymentDate", eventOccurrenceDate);
            invoice.AddTransaction(transaction);
        }


        public override List<CalculatorStrategyParameter> GetParameters()
        {
            var result = new List<CalculatorStrategyParameter>
            {
                new()
                {
                    DefaultValue = 0,
                    Label = "Taxa Educbank",
                    Type = "Percent",
                    VarName = "educbanktax"
                }
            };

            return result;
        }

        private void WriteReverseCancellation(Invoice invoice, DateTime eventOccurrenceDate, Guid eventId)
        {
            var cancellationTransaction = invoice.ValidTransactions.FirstOrDefault(x =>
                Equals(x.TransactionType, TransactionType.CancellationBeforeTransfer) ||
                Equals(x.TransactionType, TransactionType.CancellationAfterTransfer) &&
                x.EbpayMethodName == MethodName);

            if (cancellationTransaction == null)
                return;

            var cancellationTransactionList = new List<Transaction> { cancellationTransaction };

            cancellationTransactionList.AddRange(invoice.ValidTransactions.Where(x =>
                x.ReferenceId == cancellationTransaction.Id));

            var transactions = new List<Transaction>();

            foreach (var transaction in cancellationTransactionList)
            {
                var newTransaction = new Transaction(GuidGenerator.Create(), MethodName,
                    transaction.ValueCents * -1, TransactionType.ReverseCancellation, eventOccurrenceDate,
                    transaction.TransactionSide, eventId, false, transaction.Id);

                newTransaction.SetProperty("ReverseCancellationDate", transaction.OccurrenceDate);
                newTransaction.SetProperty("TransactionType",
                    Localizer[$"TransactionType:{transaction.TransactionType}:Title"].ToString().ToLower());
                transactions.Add(newTransaction);
            }

            invoice.AddTransaction(transactions);
        }

        private void WriteReversePaymentAtSchool(Invoice invoice, DateTime eventOccurrenceDate, Guid eventId)
        {
            var schoolPaymentTransaction = invoice.ValidTransactions.FirstOrDefault(x =>
                Equals(x.TransactionType, TransactionType.SchoolPaymentBeforeTransfer) ||
                Equals(x.TransactionType, TransactionType.SchoolPaymentAfterTransfer) &&
                x.EbpayMethodName == MethodName);

            if (schoolPaymentTransaction == null)
                return;

            var extraProperties = new ExtraPropertyDictionary
            {
                { "ReverseSchoolPaymentDate", schoolPaymentTransaction.OccurrenceDate },
                {
                    "TransactionType",
                    Localizer[$"TransactionType:{schoolPaymentTransaction.TransactionType}:Title"].ToString().ToLower()
                }
            };
            var transactions = new List<Transaction>
            {
                new(GuidGenerator.Create(), MethodName, schoolPaymentTransaction.ValueCents * -1,
                    TransactionType.ReverseSchoolPayment, eventOccurrenceDate, TransactionSide.School, eventId, false,
                    schoolPaymentTransaction.Id, extraProperties: extraProperties),
                new(GuidGenerator.Create(), MethodName, schoolPaymentTransaction.ValueCents,
                    TransactionType.ReverseSchoolPayment, eventOccurrenceDate, TransactionSide.Educbank, eventId, false,
                    schoolPaymentTransaction.Id, extraProperties: extraProperties)
            };

            var schoolPaymentTransactionList = new List<Transaction>();

            schoolPaymentTransactionList.AddRange(invoice.ValidTransactions.Where(x =>
                x.ReferenceId == schoolPaymentTransaction.Id));

            schoolPaymentTransactionList.AddRange(invoice.ValidTransactions.Where(x =>
                schoolPaymentTransactionList.Any(a => a.OccurrenceDate.Equals(x.OccurrenceDate)) &&
                x.TransactionType.Equals(TransactionType.IpcaCharges)));


            foreach (var transaction in schoolPaymentTransactionList)
            {
                var newTransaction = new Transaction(GuidGenerator.Create(), MethodName, transaction.ValueCents * -1,
                    TransactionType.ReverseSchoolPayment, eventOccurrenceDate, transaction.TransactionSide, eventId, false,
                    transaction.Id);

                newTransaction.SetProperty("ReverseSchoolPaymentDate", transaction.OccurrenceDate);
                newTransaction.SetProperty("TransactionType",
                    Localizer[$"TransactionType:{transaction.TransactionType}:Title"].ToString().ToLower());
                transactions.Add(newTransaction);
            }

            invoice.AddTransaction(transactions);
        }

        private void WriteEducbankTaxTransaction(Invoice invoice, Guid? referenceId, long value,
            DateTime occurrenceDate, decimal educbankTax, Guid eventid)
        {
            var educbankTaxValueCents = Convert.ToInt64(value * educbankTax);

            void SetProperties(Transaction transaction)
            {
                transaction.SetProperty("EducbankTaxPercentual", Convert.ToDouble(educbankTax));
                transaction.SetProperty("ValueBaseInReais", (double)value / 100);
            }

            if (value == 0 || educbankTax == 0)
                return;

            var transaction = new Transaction(GuidGenerator.Create(), MethodName, educbankTaxValueCents,
                TransactionType.EducbankTax, occurrenceDate, TransactionSide.School, eventid, false, referenceId);
            SetProperties(transaction);

            invoice.AddTransaction(transaction);

            transaction = new Transaction(GuidGenerator.Create(), MethodName, educbankTaxValueCents * -1,
                TransactionType.EducbankTax, occurrenceDate, TransactionSide.Educbank, eventid, false, referenceId);
            SetProperties(transaction);

            invoice.AddTransaction(transaction);
        }

        private async Task WriteIpcaChargesTransaction(Invoice invoice, DateTime eventOccurrenceDate, Guid eventId)
        {
            var ipcaChargeEnabled = await _settingsProvider.GetAsync<bool>(EducbankSettings.IpcaTaxCharge);
            var transactionType = TransactionType.IpcaCharges;
            var minimumOverduedDaysToChargeIpca =
                await _settingsProvider.GetAsync<int>(EducbankSettings.MinimumOverduedDaysToChargeIpca);
            var minimumDateToChargeIpca = EbClock.GetBrasiliaToday().AddDays(-minimumOverduedDaysToChargeIpca);
            var ipcaTotalValue = invoice.GetIpcaFineTotalCents();
            var paidDate = invoice.PaymentGatewayInvoice.PaymentDate;

            if (invoice.DueDate.Date > minimumDateToChargeIpca.Date)
                return;

            int cancelledTranfers =
                invoice.ValidTransactions.Count(x => x.TransactionType.Equals(TransactionType.CancelTransfer));
            int retentionTranfers = invoice.ValidTransactions.Count(x =>
                x.TransactionType.Equals(TransactionType.Transfer) ||
                Equals(x.TransactionType, TransactionType.Retention));

            if (cancelledTranfers < retentionTranfers)
                return;

            if (!ipcaChargeEnabled)
            {
                await _invoiceManager.ApplyIpca(invoice, true);

                if (invoice.IpcaFine.IpcaTotalCents == 0)
                    return;

                var transactionIpcaEducBankToPay = new Transaction(GuidGenerator.Create(), MethodName, -invoice.IpcaFine.IpcaTotalCents,
                    transactionType, eventOccurrenceDate, TransactionSide.Educbank, eventId);
                var transactionIpcaEducBankPayed = new Transaction(GuidGenerator.Create(), MethodName, invoice.IpcaFine.IpcaTotalCents,
                TransactionType.IpcaChargesAllowance, eventOccurrenceDate, TransactionSide.Educbank, eventId);

                transactionIpcaEducBankToPay.SetProperty("IpcaPercentual",
                    Math.Round(invoice.IpcaFine?.IpcaPercentage ?? 0, 2));
                transactionIpcaEducBankToPay.SetProperty("Dias",
                    ((paidDate ?? EbClock.Now.Date) - invoice.DueDate).Days);
                transactionIpcaEducBankPayed.SetProperty("IpcaPercentual",
                    Math.Round(invoice.IpcaFine?.IpcaPercentage ?? 0, 2));
                transactionIpcaEducBankPayed.SetProperty("Dias",
                    ((paidDate ?? EbClock.Now.Date) - invoice.DueDate).Days);


                invoice.AddTransaction(transactionIpcaEducBankPayed);
                invoice.AddTransaction(transactionIpcaEducBankToPay);
                return;
            }

            if (invoice.IpcaFine.IpcaTotalCents == 0)
                return;

            var transaction = new Transaction(GuidGenerator.Create(), MethodName, -ipcaTotalValue,
                transactionType, eventOccurrenceDate, TransactionSide.Educbank, eventId);

            transaction.SetProperty("IpcaPercentual", Math.Round(invoice.IpcaFine?.IpcaPercentage ?? 0, 2));
            transaction.SetProperty("Dias", ((paidDate ?? EbClock.Now.Date) - invoice.DueDate).Days);
            invoice.AddTransaction(transaction);

            if (ipcaTotalValue > 0)
            {
                var schoolTransaction = new Transaction(GuidGenerator.Create(), MethodName, ipcaTotalValue,
                    transactionType, eventOccurrenceDate, TransactionSide.School, eventId);

                schoolTransaction.SetProperty("IpcaPercentual", Math.Round(invoice.IpcaFine?.IpcaPercentage ?? 0, 2));
                schoolTransaction.SetProperty("Dias", ((paidDate ?? EbClock.Now.Date) - invoice.DueDate).Days);
                invoice.AddTransaction(schoolTransaction);
            }
        }


        private async Task WritePaymentChargesTransaction(Invoice invoice, DateTime eventOccurrenceDate, Guid referenceId,
            TransactionType transactionType, long receivedCharges, TransactionSide side, long baseValue, Guid eventId)
        {
            var educbankPaymentFine = await GetEducbankPaymentFine();
            var dateToCompare = invoice.PaymentGatewayInvoice.PaymentDate ?? EbClock.Now;

            var totalFineCents = educbankPaymentFine.GetPaymentFineCents(invoice.DateToCalculateTransfer,
                invoice.ValueToCalculateTransfer, dateToCompare.Date);

            var chargesDiff = totalFineCents - receivedCharges;

            if (totalFineCents > 0 && chargesDiff != 0)
            {
                if (side == TransactionSide.Educbank)
                    chargesDiff = totalFineCents * -1;

                var transaction = new Transaction(GuidGenerator.Create(), MethodName, chargesDiff, transactionType,
                    eventOccurrenceDate, side, eventId, false, referenceId);

                SetPropertiesStrategy(invoice, transaction, educbankPaymentFine, dateToCompare, baseValue);

                invoice.AddTransaction(transaction);
            }
        }

        private static void SetPropertiesStrategy(Invoice invoice, Transaction transaction,
            PaymentFine educbankPaymentFine, DateTime dateToCompare, long baseValue)
        {
            var fineValue = educbankPaymentFine.GetOverdueFineCents(baseValue);
            var dailyInterestValue = educbankPaymentFine.GetDailyInterestCents(baseValue);
            var daysOfDelay = PaymentFine.CalculateDaysOfDelay(invoice.DateToCalculateTransfer, dateToCompare);

            transaction.SetProperty("FineValueInReais", (double)fineValue / 100);
            transaction.SetProperty("OverdueFine", educbankPaymentFine.OverdueFine);
            transaction.SetProperty("DailyInterestValueInReais", (double)dailyInterestValue / 100);
            transaction.SetProperty("DailyInterest", educbankPaymentFine.DailyInterest);
            transaction.SetProperty("BaseValueInReais", (double)baseValue / 100);
            transaction.SetProperty("DaysOfDelay", daysOfDelay);
        }

        private async Task WriteCancellationChargesTransaction(Invoice invoice, DateTime eventOccurrenceDate, Guid referenceId,
            Guid eventId)
        {
            var educbankPaymentFine = await GetEducbankPaymentFine();
            var totalFineCents = educbankPaymentFine.GetPaymentFineCents(invoice.DateToCalculateTransfer,
                invoice.ValueToCalculateTransfer, eventOccurrenceDate);

            if (totalFineCents > 0)
            {
                var transaction = new Transaction(GuidGenerator.Create(), MethodName, totalFineCents,
                    TransactionType.CancellationCharges, eventOccurrenceDate, TransactionSide.School, eventId, false,
                    referenceId);
                var dateToCompare = invoice.PaymentGatewayInvoice.CancelationDate ?? EbClock.Now;
                SetPropertiesStrategy(invoice, transaction, educbankPaymentFine, dateToCompare,
                    invoice.ValueToCalculateTransfer);
                invoice.AddTransaction(transaction);

                transaction = new Transaction(GuidGenerator.Create(), MethodName, totalFineCents * -1,
                    TransactionType.CancellationCharges, eventOccurrenceDate, TransactionSide.Educbank, eventId, false,
                    referenceId);
                SetPropertiesStrategy(invoice, transaction, educbankPaymentFine, dateToCompare,
                    invoice.ValueToCalculateTransfer);
                invoice.AddTransaction(transaction);
            }
        }
        private async Task WriteChargesTransaction(Invoice invoice, DateTime eventOccurrenceDate, Guid referenceId,
            TransactionType transferType, Guid eventId)
        {
            var educbankPaymentFine = await GetEducbankPaymentFine();
            var totalFineCents = educbankPaymentFine.GetPaymentFineCents(invoice.DateToCalculateTransfer,
                invoice.ValueToCalculateTransfer, eventOccurrenceDate);

            if (totalFineCents > 0)
            {
                var transaction = new Transaction(GuidGenerator.Create(), MethodName, totalFineCents, transferType,
                    eventOccurrenceDate, TransactionSide.School, eventId, false, referenceId);

                var dateToCompare = invoice.PaymentGatewayInvoice.PaymentDate ?? EbClock.Now;
                SetPropertiesStrategy(invoice, transaction, educbankPaymentFine, dateToCompare,
                    invoice.ValueToCalculateTransfer);
                invoice.AddTransaction(transaction);

                transaction = new Transaction(GuidGenerator.Create(), MethodName, totalFineCents * -1,
                    transferType, eventOccurrenceDate, TransactionSide.Educbank, eventId, false,
                    referenceId);
                SetPropertiesStrategy(invoice, transaction, educbankPaymentFine, dateToCompare,
                    invoice.ValueToCalculateTransfer);
                invoice.AddTransaction(transaction);
            }
        }
        private void WriteBankFeeTransaction(Invoice invoice, DateTime eventOccurrenceDate, Guid eventId)
        {
            if (invoice.PaymentGatewayInvoice?.TaxesPaidCents == null ||
                invoice.PaymentGatewayInvoice?.PaidMethod == InvoicePaymentMethod.CreditCard)
                return;
            var valueCents = invoice.PaymentGatewayInvoice.TaxesPaidCents.Value;
            var transaction = new Transaction(GuidGenerator.Create(), MethodName, valueCents,
                TransactionType.BankFeeProvision, eventOccurrenceDate, TransactionSide.Educbank, eventId);

            invoice.AddTransaction(transaction);

            transaction = new Transaction(GuidGenerator.Create(), MethodName, valueCents * -1, TransactionType.BankFee,
                eventOccurrenceDate, TransactionSide.Educbank, eventId);

            invoice.AddTransaction(transaction);
        }

        private async Task<PaymentFine> GetEducbankPaymentFine()
        {
            var overdueFine = await SettingProvider.GetAsync<decimal>(EducbankSettings.OverdueFine);
            var dailyInterest = await SettingProvider.GetAsync<decimal>(EducbankSettings.DailyInterest);
            var educbankPaymentFine = new PaymentFine(overdueFine, dailyInterest);

            return educbankPaymentFine;
        }

        private async Task<decimal> GetEducbankTax(string companyCnpj)
        {
            var school = await SchoolManager.GetSchoolByCNPJ(companyCnpj);
            var plan = school.EbpayPlans.First(x => x.IsDefault);
            var educbankTaxString = plan.Parameters.GetOrDefault(EducbankTaxPropertyName);
            educbankTaxString = educbankTaxString?.Replace(",", ".");

            if (!decimal.TryParse(educbankTaxString, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture,
                    out var educbankTax) || educbankTax <= 0)
            {
                throw new BusinessException(ExceptionConsts.InvalidEducbankTax).WithData("taxa", educbankTaxString);
            }

            return educbankTax;
        }

        private async Task<bool> ShouldWriteTransaction(InvoiceEventType eventType, Invoice invoice)
        {
            if (eventType != InvoiceEventType.Created &&
                (invoice.ValidTransactions == null || invoice.ValidTransactions.Count == 0))
                return false;

            var school = await SchoolManager.GetSchoolByCNPJ(invoice.CompanyCnpj.Value);

            if (school.EbpayPlans == null || school.EbpayPlans.Count == 0)
                return false;

            return invoice.PlannedInstallments[0].EbpayPlan != CalculatorStrategyEnum.Gateway;
        }

        /// <summary>
        /// Verifica se pode ser lançado algum tipo de transação cujo motivo é um novo contrato
        /// de um estudante ou RF inadimplente
        /// </summary>
        /// <param name="invoice"></param>
        /// <returns></returns>
        private static bool CanWriteRetentionByContractWithDebtorTransaction(Invoice invoice)
        {
            return
                invoice.Transactions.Count(x =>
                    x.TransactionType.Equals(TransactionType.NewInvoiceByGuardianDocument) && !x.IsCanceled) ==
                invoice.Transactions.Count(x =>
                    x.TransactionType.Equals(TransactionType.NewInvoiceByGuardianDocumentPayback) && !x.IsCanceled) &&
                invoice.Transactions.Count(x =>
                    x.TransactionType.Equals(TransactionType.NewEnrollmentByGuardianDocument) && !x.IsCanceled) ==
                invoice.Transactions.Count(x =>
                    x.TransactionType.Equals(TransactionType.NewEnrollmentByGuardianDocumentPayback) &&
                    !x.IsCanceled) &&
                invoice.Transactions.Count(x =>
                    x.TransactionType.Equals(TransactionType.ReEnrollment) && !x.IsCanceled) ==
                invoice.Transactions.Count(x =>
                    x.TransactionType.Equals(TransactionType.ReEnrollmentCanceled) && !x.IsCanceled);
        }

        public override async Task WritePaymentByCreditCardTransactions(Invoice invoice, DateTime eventOccurrenceDate,
            long creditCardTaxValueCents, Guid eventId)
        {
            await AddInvoicePaymentTransactions(invoice, eventOccurrenceDate, eventId);

            var chargeResponsible = await SettingProvider.GetOrNullAsync(EducbankSettings.ResponsibleForCreditCardTax);
            var parseResult = Enum.TryParse(chargeResponsible, out ResponsibleForCreditCardTaxType responsible);
            if (!parseResult)
                return;
            switch (responsible)
            {
                case ResponsibleForCreditCardTaxType.Guardian:
                {
                    var transaction = new Transaction(GuidGenerator.Create(), MethodName, creditCardTaxValueCents * -1,
                        TransactionType.CreditCardTax, eventOccurrenceDate, TransactionSide.Educbank, eventId);
                    transaction.SetProperty("InvoiceValue", (double)invoice.TotalCents / 100);
                    transaction.SetProperty("CreditCardTaxCents", (double)creditCardTaxValueCents / 100);
                    invoice.AddTransaction(transaction);
                    break;
                }
                case ResponsibleForCreditCardTaxType.School:
                {
                    var transaction = new Transaction(GuidGenerator.Create(), MethodName, creditCardTaxValueCents * -1,
                        TransactionType.CreditCardTax, eventOccurrenceDate, TransactionSide.Educbank, eventId);
                    transaction.SetProperty("InvoiceValue", (double)invoice.TotalCents / 100);
                    transaction.SetProperty("CreditCardTaxCents", (double)creditCardTaxValueCents / 100);
                    invoice.AddTransaction(transaction);

                    transaction = new Transaction(GuidGenerator.Create(), MethodName, creditCardTaxValueCents,
                        TransactionType.CreditCardTax, eventOccurrenceDate, TransactionSide.School, eventId);
                    transaction.SetProperty("InvoiceValue", (double)invoice.TotalCents / 100);
                    transaction.SetProperty("CreditCardTaxCents", (double)creditCardTaxValueCents / 100);

                    invoice.AddTransaction(transaction);
                    break;
                }
                case ResponsibleForCreditCardTaxType.Educbank:
                    break;
                case ResponsibleForCreditCardTaxType.Undefined:
                default:
                    throw new BusinessException(ExceptionConsts.ZeroDefaultStrategy
                        .InvalidResponsibleForCreditCardTaxType);
            }
        }

        private static bool CheckIfHasRetentionByEnrollment(Invoice invoice)
        {
            var reEnrollmentTransactionsCount = invoice.Transactions.Count(x =>
                Equals(x.TransactionType, TransactionType.ReEnrollment) ||
                Equals(x.TransactionType, TransactionType.NewEnrollmentByGuardianDocument) ||
                Equals(x.TransactionType, TransactionType.NewInvoiceByGuardianDocument));

            var reEnrollmentPayBackTransactionsCount = invoice.Transactions.Count(x =>
                Equals(x.TransactionType, TransactionType.ReEnrollmentPayback) ||
                Equals(x.TransactionType, TransactionType.NewEnrollmentByGuardianDocumentPayback) ||
                Equals(x.TransactionType, TransactionType.NewInvoiceByGuardianDocumentPayback));

            return reEnrollmentTransactionsCount > reEnrollmentPayBackTransactionsCount;
        }
    }
}
