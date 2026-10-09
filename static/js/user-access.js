/* ==========================================================================
   User access management workspace
   ========================================================================== */
(function () {
  'use strict';

  var COMPANY_NAME = 'brilliant chemicals pvt ltd';

  function escapeHtml(value) {
    return String(value === null || value === undefined ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function modal(id) {
    var element = document.getElementById(id);
    return element ? new bootstrap.Modal(element) : null;
  }

  var addModal = modal('addEmployeeModal');
  var removeModal = modal('removeEmployeeModal');
  var roleModal = modal('roleModal');
  var departmentModal = modal('departmentModal');
  var approverModal = modal('approverModal');
  var balanceModal = modal('balanceModal');

  function showAlert(id, text, kind) {
    var box = document.getElementById(id);
    var label = document.getElementById(id + 'Text');
    if (!box || !label) return;
    label.textContent = text;
    box.className = 'ps-alert is-' + (kind || 'info');
    box.style.display = 'flex';
  }

  function hideAlerts(ids) {
    ids.forEach(function (id) {
      var box = document.getElementById(id);
      if (box) box.style.display = 'none';
    });
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

  /* --------------------------------------------------------------- adding */
  var openAdd = document.getElementById('openAddEmployee');
  if (openAdd && addModal) {
    openAdd.addEventListener('click', function () {
      document.getElementById('addEmployeeForm').reset();
      hideAlerts(['addEmployeeAlert']);
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
            setTimeout(function () { window.location.reload(); }, 900);
          } else {
            return response.text().then(function (text) {
              showAlert('addEmployeeAlert', text || 'Could not add the employee.', 'error');
            });
          }
        })
        .catch(function () { showAlert('addEmployeeAlert', 'Could not add the employee.', 'error'); });
    });
  }

  /* ------------------------------------------------------------ removing */
  var openRemove = document.getElementById('openRemoveEmployee');
  if (openRemove && removeModal) {
    openRemove.addEventListener('click', function () {
      hideAlerts(['removeEmployeeAlert']);
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
            setTimeout(function () { window.location.reload(); }, 900);
          } else {
            showAlert('removeEmployeeAlert', result.message || 'Could not remove the selected employees.', 'error');
          }
        })
        .catch(function () { showAlert('removeEmployeeAlert', 'Could not remove the selected employees.', 'error'); });
    });
  }

  /* ----------------------------------------------------------- row edits */
  document.addEventListener('click', function (event) {
    var role = event.target.closest('.edit-priv-btn');
    if (role && roleModal) {
      event.preventDefault();
      document.getElementById('roleCurrentId').value = role.dataset.ID || role.dataset.id || '';
      hideAlerts(['roleAlert']);
      roleModal.show();
      return;
    }

    var department = event.target.closest('.edit-department-btn');
    if (department && departmentModal) {
      event.preventDefault();
      document.getElementById('departmentCurrentId').value = department.dataset.ID || department.dataset.id || '';
      hideAlerts(['departmentAlert']);
      departmentModal.show();
      return;
    }

    var approver = event.target.closest('.change-approver-btn');
    if (approver && approverModal) {
      event.preventDefault();
      document.getElementById('approverCurrentId').value = approver.dataset.ID || approver.dataset.id || '';
      hideAlerts(['approverAlert']);
      approverModal.show();
      return;
    }

    var balance = event.target.closest('.edit-balance-btn');
    if (balance && balanceModal) {
      event.preventDefault();
      document.getElementById('balanceCurrentId').value = balance.dataset.ID || balance.dataset.id || '';
      hideAlerts(['balanceAlert']);
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
          setTimeout(function () { window.location.reload(); }, 900);
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
          setTimeout(function () { window.location.reload(); }, 900);
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
          setTimeout(function () { window.location.reload(); }, 900);
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
          setTimeout(function () { window.location.reload(); }, 900);
        } else {
          showAlert('balanceAlert', (payload && (payload.message || payload.error)) || 'Could not update the balance.', 'error');
        }
      });
    });
  }

  /* --------------------------------------------------------- portal access */
  var matrixHead = document.getElementById('accessMatrixHead');
  var matrixBody = document.getElementById('accessMatrixBody');
  var matrixModules = [];

  function loadMatrix() {
    if (!matrixBody) return;
    fetch('/api/access/matrix')
      .then(function (response) { return response.json(); })
      .then(function (payload) {
        if (!payload.success) {
          matrixBody.innerHTML = '<tr><td class="ps-muted" colspan="3">' +
            escapeHtml(payload.message || 'Unable to load portal access.') + '</td></tr>';
          return;
        }
        matrixModules = payload.modules || [];
        renderMatrix(payload.employees || []);
      })
      .catch(function () {
        matrixBody.innerHTML = '<tr><td class="ps-muted" colspan="3">Unable to load portal access.</td></tr>';
      });
  }

  function renderMatrix(employees) {
    var columns = 3 + matrixModules.length;
    matrixHead.innerHTML = '<tr><th>Employee</th><th>Role</th><th>Department</th>' +
      matrixModules.map(function (module) { return '<th>' + escapeHtml(module.title) + '</th>'; }).join('') +
      '</tr>';

    if (!employees.length) {
      matrixBody.innerHTML = '<tr><td class="ps-muted" colspan="' + columns + '">No employees found.</td></tr>';
      return;
    }

    matrixBody.innerHTML = employees.map(function (employee) {
      var cells = matrixModules.map(function (module) {
        var enabled = !!(employee.modules && employee.modules[module.key]);
        return '<td class="ps-access-cell">' +
          '<label class="ps-switch" title="' + escapeHtml(module.title) + '">' +
          '<input type="checkbox" data-employee="' + employee.id + '" data-module="' + escapeHtml(module.key) + '"' +
          (enabled ? ' checked' : '') + ' aria-label="' + escapeHtml(module.title) + ' for ' + escapeHtml(employee.name) + '">' +
          '<span class="ps-switch-track"></span>' +
          '</label></td>';
      }).join('');

      return '<tr>' +
        '<td><strong>' + escapeHtml(employee.name) + '</strong>' +
        '<div class="ps-muted" style="font-size:0.72rem;">#' + escapeHtml(employee.id) + '</div></td>' +
        '<td>' + escapeHtml(employee.role) + '</td>' +
        '<td>' + escapeHtml(employee.department) + '</td>' +
        cells +
        '</tr>';
    }).join('');
  }

  if (matrixBody) {
    matrixBody.addEventListener('change', function (event) {
      var checkbox = event.target.closest('input[data-module]');
      if (!checkbox) return;
      var wrapper = checkbox.closest('.ps-switch');
      var restore = checkbox.checked;
      wrapper.classList.add('is-busy');

      fetch('/api/access/toggle', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          employee_id: parseInt(checkbox.getAttribute('data-employee'), 10),
          module: checkbox.getAttribute('data-module'),
          enabled: checkbox.checked
        })
      })
        .then(function (response) { return response.json(); })
        .then(function (result) {
          if (result.success) {
            showAlert('accessAlert', 'Portal access updated.', 'ok');
          } else {
            checkbox.checked = !restore;
            showAlert('accessAlert', result.message || 'Could not update portal access.', 'error');
          }
        })
        .catch(function () {
          checkbox.checked = !restore;
          showAlert('accessAlert', 'Could not update portal access.', 'error');
        })
        .finally(function () { wrapper.classList.remove('is-busy'); });
    });

    var refresh = document.getElementById('refreshAccessMatrix');
    if (refresh) refresh.addEventListener('click', loadMatrix);

    loadMatrix();
  }
})();
