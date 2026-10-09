/* ==========================================================================
   HR & Payroll workspace
   ========================================================================== */
(function () {
  'use strict';

  var settingsModalEl = document.getElementById('currencySettingsModal');
  var payrollModalEl = document.getElementById('editPayrollModal');
  var hrModalEl = document.getElementById('editHrModal');
  var uploadModalEl = document.getElementById('payrollUploadModal');

  var settingsModal = settingsModalEl ? new bootstrap.Modal(settingsModalEl) : null;
  var payrollModal = payrollModalEl ? new bootstrap.Modal(payrollModalEl) : null;
  var hrModal = hrModalEl ? new bootstrap.Modal(hrModalEl) : null;
  var uploadModal = uploadModalEl ? new bootstrap.Modal(uploadModalEl) : null;

  var settings = {};
  try {
    settings = JSON.parse(document.getElementById('currencySettingsData').textContent || '{}') || {};
  } catch (error) {
    settings = {};
  }

  function setText(id, value) {
    var node = document.getElementById(id);
    if (node) node.textContent = value;
  }

  function paintSettings() {
    var split = settings.splitEnabled && settings.splitConfig;
    setText('hrBaseCurrency', settings.baseCurrency || '--');
    setText('hrSplitLabel', split ? 'Enabled' : 'Off');
    setText('hrSplitNote', split
      ? (settings.splitConfig.usdPercent + '% USD / ' + settings.splitConfig.zwgPercent + '% ZWG')
      : 'Full salary in base currency');
    setText('hrUsdPct', split ? settings.splitConfig.usdPercent + '%' : '--%');
    setText('hrZwgPct', split ? settings.splitConfig.zwgPercent + '%' : '--%');
    setText('hrRate', split && settings.splitConfig.exchangeRate ? settings.splitConfig.exchangeRate : '--');
  }

  paintSettings();

  function showAlert(id, text, kind) {
    var box = document.getElementById(id);
    var label = document.getElementById(id + 'Text');
    if (!box || !label) return;
    label.textContent = text;
    box.className = 'ps-alert is-' + (kind || 'info');
    box.style.display = 'flex';
  }

  /* ------------------------------------------------------------- settings */
  var openCurrency = document.getElementById('openCurrencyModal');
  if (openCurrency && settingsModal) {
    openCurrency.addEventListener('click', function () {
      document.getElementById('baseCurrencySelect').value = settings.baseCurrency || 'USD';
      var splitEnabled = document.getElementById('splitEnabled');
      var splitFields = document.getElementById('splitFields');
      splitEnabled.checked = !!settings.splitEnabled;
      splitFields.style.display = settings.splitEnabled ? 'block' : 'none';
      if (settings.splitConfig) {
        document.getElementById('usdPercent').value = settings.splitConfig.usdPercent;
        document.getElementById('zwgPercent').value = settings.splitConfig.zwgPercent;
        document.getElementById('exchangeRate').value = settings.splitConfig.exchangeRate || 0;
      }
      document.getElementById('currencyAlert').style.display = 'none';
      settingsModal.show();
    });
  }

  var splitToggle = document.getElementById('splitEnabled');
  if (splitToggle) {
    splitToggle.addEventListener('change', function () {
      document.getElementById('splitFields').style.display = splitToggle.checked ? 'block' : 'none';
    });
  }

  var settingsForm = document.getElementById('currencySettingsForm');
  if (settingsForm) {
    settingsForm.addEventListener('submit', function (event) {
      event.preventDefault();
      var splitEnabled = document.getElementById('splitEnabled').checked;
      var payload = {
        baseCurrency: document.getElementById('baseCurrencySelect').value,
        splitEnabled: splitEnabled,
        splitConfig: splitEnabled ? {
          usdPercent: parseFloat(document.getElementById('usdPercent').value) || 0,
          zwgPercent: parseFloat(document.getElementById('zwgPercent').value) || 0,
          exchangeRate: parseFloat(document.getElementById('exchangeRate').value) || 0
        } : null
      };

      fetch('/save-currency-settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      })
        .then(function (response) { return response.json(); })
        .then(function (result) {
          if (result.success) {
            settings = payload;
            paintSettings();
            showAlert('currencyAlert', result.message || 'Configuration saved.', 'ok');
            setTimeout(function () { settingsModal.hide(); }, 900);
          } else {
            showAlert('currencyAlert', result.message || 'Could not save the configuration.', 'error');
          }
        })
        .catch(function () { showAlert('currencyAlert', 'Could not save the configuration.', 'error'); });
    });
  }

  /* ----------------------------------------------------- payroll editing */
  document.addEventListener('click', function (event) {
    var payrollButton = event.target.closest('.edit-emp-details-comp-btn-payroll');
    if (payrollButton) {
      event.preventDefault();
      var data = payrollButton.dataset;
      document.getElementById('payrollId').value = data.id || '';
      document.getElementById('editPayrollName').textContent =
        'Editing payroll details for ' + (data.firstname || '') + ' ' + (data.surname || '');
      document.getElementById('payrollDateJoined').value = (data.datejoined || '').slice(0, 10);
      document.getElementById('payrollDepartment').value = data.department || '';
      document.getElementById('payrollDesignation').value = data.designation || '';
      document.getElementById('payrollManager').value = data.manager || '';
      document.getElementById('payrollSalary').value = data.basicsalary || '';
      document.getElementById('payrollCurrency').value = (data.basecurrency || 'USD').toUpperCase();
      document.getElementById('payrollBank').value = data.bank || '';
      document.getElementById('payrollBranch').value = data.branch || '';
      document.getElementById('payrollAlert').style.display = 'none';
      payrollModal.show();
      return;
    }

    var hrButton = event.target.closest('.edit-emp-details-comp-btn');
    if (hrButton) {
      event.preventDefault();
      var hr = hrButton.dataset;
      document.getElementById('hrId').value = hr.id || '';
      document.getElementById('hrFirstname').value = hr.firstname || '';
      document.getElementById('hrSurname').value = hr.surname || '';
      document.getElementById('hrWhatsapp').value = hr.whatsapp || '';
      document.getElementById('hrEmail').value = hr.email || '';
      document.getElementById('hrAddress').value = hr.address || '';
      document.getElementById('hrAlert').style.display = 'none';
      hrModal.show();
    }
  });

  var payrollForm = document.getElementById('editPayrollForm');
  if (payrollForm) {
    payrollForm.addEventListener('submit', function (event) {
      event.preventDefault();
      fetch('/update_employee_details_payroll', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          payroll_id: document.getElementById('payrollId').value,
          datejoined: document.getElementById('payrollDateJoined').value,
          department: document.getElementById('payrollDepartment').value,
          designation: document.getElementById('payrollDesignation').value,
          manager: document.getElementById('payrollManager').value,
          basicsalary: document.getElementById('payrollSalary').value,
          basecurrency: document.getElementById('payrollCurrency').value,
          bank: document.getElementById('payrollBank').value,
          branch: document.getElementById('payrollBranch').value
        })
      })
        .then(function (response) { return response.json(); })
        .then(function (result) {
          if (result.success) {
            showAlert('payrollAlert', result.message || 'Payroll details saved.', 'ok');
            setTimeout(function () { window.location.reload(); }, 800);
          } else {
            showAlert('payrollAlert', result.message || result.error || 'Could not save the details.', 'error');
          }
        })
        .catch(function () { showAlert('payrollAlert', 'Could not save the details.', 'error'); });
    });
  }

  var hrForm = document.getElementById('editHrForm');
  if (hrForm) {
    hrForm.addEventListener('submit', function (event) {
      event.preventDefault();
      fetch('/update_employee_details_admin_comp', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id: document.getElementById('hrId').value,
          firstname: document.getElementById('hrFirstname').value.trim(),
          surname: document.getElementById('hrSurname').value.trim(),
          whatsapp: document.getElementById('hrWhatsapp').value.trim(),
          email: document.getElementById('hrEmail').value.trim(),
          address: document.getElementById('hrAddress').value.trim()
        })
      })
        .then(function (response) { return response.json(); })
        .then(function (result) {
          if (result.success) {
            showAlert('hrAlert', result.message || 'Employee updated.', 'ok');
            setTimeout(function () { window.location.reload(); }, 800);
          } else {
            showAlert('hrAlert', result.error || result.message || 'Could not update the employee.', 'error');
          }
        })
        .catch(function () { showAlert('hrAlert', 'Could not update the employee.', 'error'); });
    });
  }

  /* ------------------------------------------------------------- uploads */
  var openUpload = document.getElementById('openPayrollUpload');
  if (openUpload && uploadModal) {
    openUpload.addEventListener('click', function () {
      document.getElementById('payrollUploadAlert').style.display = 'none';
      uploadModal.show();
    });
  }

  var templateButton = document.getElementById('downloadPayrollTemplate');
  if (templateButton) {
    templateButton.addEventListener('click', function () {
      window.location.href = '/download-excel-template-payroll-employees';
    });
  }

  var uploadForm = document.getElementById('payrollUploadForm');
  if (uploadForm) {
    uploadForm.addEventListener('submit', function (event) {
      event.preventDefault();
      var body = new FormData();
      var file = document.getElementById('payrollFile').files[0];
      if (!file) {
        showAlert('payrollUploadAlert', 'Choose a payroll workbook first.', 'error');
        return;
      }
      body.append('file-payroll', file);

      fetch('/upload-excel-payroll', { method: 'POST', body: body })
        .then(function (response) { return response.json(); })
        .then(function (result) {
          var message = result.message || 'Upload processed.';
          if (result.status === 'success') {
            showAlert('payrollUploadAlert', message, 'ok');
            setTimeout(function () { window.location.reload(); }, 1000);
          } else {
            showAlert('payrollUploadAlert', message, 'error');
          }
        })
        .catch(function () { showAlert('payrollUploadAlert', 'The upload failed. Please try again.', 'error'); });
    });
  }
})();
