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

  /* ------------------------------------------------- employee administration */
  var COMPANY_NAME = 'brilliant chemicals pvt ltd';

  function modalFor(id) {
    var element = document.getElementById(id);
    return element ? new bootstrap.Modal(element) : null;
  }

  function hideAlert(id) {
    var box = document.getElementById(id);
    if (box) box.style.display = 'none';
  }

  function reloadSoon() {
    setTimeout(function () { window.location.reload(); }, 900);
  }

  function postForm(url, data, onDone) {
    fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams(data).toString()
    })
      .then(function (response) {
        return response.text().then(function (text) {
          var payload = null;
          try { payload = JSON.parse(text); } catch (error) { payload = null; }
          onDone(response.ok, payload, text);
        });
      })
      .catch(function () { onDone(false, { message: 'Request failed. Please try again.' }, ''); });
  }

  var addModal = modalFor('addEmployeeModal');
  var removeModal = modalFor('removeEmployeeModal');
  var roleModal = modalFor('roleModal');
  var departmentModal = modalFor('departmentModal');
  var approverModal = modalFor('approverModal');
  var balanceModal = modalFor('balanceModal');

  var openAdd = document.getElementById('openAddEmployee');
  if (openAdd && addModal) {
    openAdd.addEventListener('click', function () {
      document.getElementById('addEmployeeForm').reset();
      hideAlert('addEmployeeAlert');
      addModal.show();
    });
  }

  var addForm = document.getElementById('addEmployeeForm');
  if (addForm) {
    addForm.addEventListener('submit', function (event) {
      event.preventDefault();
      var body = new FormData();
      body.append('firstname', document.getElementById('addFirstname').value.trim());
      body.append('surname', document.getElementById('addSurname').value.trim());
      body.append('whatsapp', document.getElementById('addWhatsapp').value.trim());
      body.append('email', document.getElementById('addEmail').value.trim());
      body.append('role', document.getElementById('addRole').value);
      body.append('department', document.getElementById('addDepartment').value);
      body.append('selected_employees', document.getElementById('addApprover').value);
      body.append('currentleavedays', document.getElementById('addBalance').value);
      body.append('monthlyaccumulation', document.getElementById('addAccumulation').value);

      fetch('/manual_add_employee', { method: 'POST', body: body })
        .then(function (response) {
          if (response.ok) {
            showAlert('addEmployeeAlert', 'Employee added successfully.', 'ok');
            reloadSoon();
          } else {
            return response.text().then(function (text) {
              showAlert('addEmployeeAlert', text || 'Could not add the employee.', 'error');
            });
          }
        })
        .catch(function () { showAlert('addEmployeeAlert', 'Could not add the employee.', 'error'); });
    });
  }

  var openRemove = document.getElementById('openRemoveEmployee');
  if (openRemove && removeModal) {
    openRemove.addEventListener('click', function () {
      hideAlert('removeEmployeeAlert');
      removeModal.show();
    });
  }

  var confirmRemove = document.getElementById('confirmRemoveEmployees');
  if (confirmRemove) {
    confirmRemove.addEventListener('click', function () {
      var ids = Array.prototype.slice
        .call(document.querySelectorAll('#removeEmployeeModal .employee-checkbox:checked'))
        .map(function (checkbox) { return parseInt(checkbox.value, 10); });

      if (!ids.length) {
        showAlert('removeEmployeeAlert', 'Select at least one employee.', 'error');
        return;
      }

      fetch('/remove_employees', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ employee_ids: ids, companyname: COMPANY_NAME })
      })
        .then(function (response) { return response.json(); })
        .then(function (result) {
          if (result.success) {
            showAlert('removeEmployeeAlert', result.message || 'Employees removed.', 'ok');
            reloadSoon();
          } else {
            showAlert('removeEmployeeAlert', result.message || 'Could not remove the selected employees.', 'error');
          }
        })
        .catch(function () { showAlert('removeEmployeeAlert', 'Could not remove the selected employees.', 'error'); });
    });
  }

  /* ------------------------------------------------- employee row actions */
  document.addEventListener('click', function (event) {
    var role = event.target.closest('.edit-priv-btn');
    if (role && roleModal) {
      event.preventDefault();
      document.getElementById('roleCurrentId').value = role.dataset.ID || role.dataset.id || '';
      hideAlert('roleAlert');
      roleModal.show();
      return;
    }

    var department = event.target.closest('.edit-department-btn');
    if (department && departmentModal) {
      event.preventDefault();
      document.getElementById('departmentCurrentId').value = department.dataset.ID || department.dataset.id || '';
      hideAlert('departmentAlert');
      departmentModal.show();
      return;
    }

    var approver = event.target.closest('.change-approver-btn');
    if (approver && approverModal) {
      event.preventDefault();
      document.getElementById('approverCurrentId').value = approver.dataset.ID || approver.dataset.id || '';
      hideAlert('approverAlert');
      approverModal.show();
      return;
    }

    var balance = event.target.closest('.edit-balance-btn');
    if (balance && balanceModal) {
      event.preventDefault();
      document.getElementById('balanceCurrentId').value = balance.dataset.ID || balance.dataset.id || '';
      hideAlert('balanceAlert');
      balanceModal.show();
    }
  });

  var saveRole = document.getElementById('saveRole');
  if (saveRole) {
    saveRole.addEventListener('click', function () {
      postForm('/update_role', {
        role: document.getElementById('roleSelect').value,
        currentId: document.getElementById('roleCurrentId').value
      }, function (ok, payload) {
        if (ok) {
          showAlert('roleAlert', (payload && payload.message) || 'Role updated.', 'ok');
          reloadSoon();
        } else {
          showAlert('roleAlert', (payload && (payload.message || payload.error)) || 'Could not update the role.', 'error');
        }
      });
    });
  }

  var saveDepartment = document.getElementById('saveDepartment');
  if (saveDepartment) {
    saveDepartment.addEventListener('click', function () {
      postForm('/update_department', {
        department: document.getElementById('departmentSelect').value,
        currentId: document.getElementById('departmentCurrentId').value,
        companyname: COMPANY_NAME
      }, function (ok, payload) {
        if (ok) {
          showAlert('departmentAlert', (payload && payload.message) || 'Department updated.', 'ok');
          reloadSoon();
        } else {
          showAlert('departmentAlert', (payload && (payload.message || payload.error)) || 'Could not update the department.', 'error');
        }
      });
    });
  }

  var saveApprover = document.getElementById('saveApprover');
  if (saveApprover) {
    saveApprover.addEventListener('click', function () {
      var approver = document.getElementById('approverSelect').value;
      if (!approver) {
        showAlert('approverAlert', 'Choose an approver first.', 'error');
        return;
      }
      postForm('/update_approver', {
        approver: approver,
        currentIdapprover: document.getElementById('approverCurrentId').value
      }, function (ok, payload) {
        if (ok) {
          showAlert('approverAlert', (payload && payload.message) || 'Approver updated.', 'ok');
          reloadSoon();
        } else {
          showAlert('approverAlert', (payload && (payload.message || payload.error)) || 'Could not update the approver.', 'error');
        }
      });
    });
  }

  var saveBalance = document.getElementById('saveBalance');
  if (saveBalance) {
    saveBalance.addEventListener('click', function () {
      postForm('/update_balance', {
        balance: document.getElementById('balanceInput').value,
        currentIdbalance: document.getElementById('balanceCurrentId').value
      }, function (ok, payload) {
        if (ok) {
          showAlert('balanceAlert', (payload && payload.message) || 'Balance updated.', 'ok');
          reloadSoon();
        } else {
          showAlert('balanceAlert', (payload && (payload.message || payload.error)) || 'Could not update the balance.', 'error');
        }
      });
    });
  }
})();
