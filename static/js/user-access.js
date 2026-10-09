/* ==========================================================================
   User access management workspace — portal access toggles
   ========================================================================== */
(function () {
  'use strict';

  function escapeHtml(value) {
    return String(value === null || value === undefined ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function showAlert(text, kind) {
    var box = document.getElementById('accessAlert');
    var label = document.getElementById('accessAlertText');
    if (!box || !label) return;
    label.textContent = text;
    box.className = 'ps-alert is-' + (kind || 'info');
    box.style.display = 'flex';
  }

  function hideAlert() {
    var box = document.getElementById('accessAlert');
    if (box) box.style.display = 'none';
  }

  var matrixHead = document.getElementById('accessMatrixHead');
  var matrixBody = document.getElementById('accessMatrixBody');
  var managerList = document.getElementById('accessManagersList');
  var managerChip = document.getElementById('accessManagerChip');
  var grantSelect = document.getElementById('grantManagerSelect');
  var grantButton = document.getElementById('grantManagerButton');
  var matrixModules = [];
  var matrixEmployees = [];

  function loadMatrix() {
    if (!matrixBody) return;
    hideAlert();
    fetch('/api/access/matrix')
      .then(function (response) { return response.json(); })
      .then(function (payload) {
        if (!payload.success) {
          matrixBody.innerHTML = '<tr><td class="ps-muted" colspan="3">' +
            escapeHtml(payload.message || 'Unable to load portal access.') + '</td></tr>';
          return;
        }
        matrixModules = payload.modules || [];
        matrixEmployees = payload.employees || [];
        renderMatrix(matrixEmployees);
        renderManagers();
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
          '<label class="ps-switch" title="' + escapeHtml(module.full_title || module.title) + '">' +
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

  function isManager(employee) {
    return !!(employee.modules && employee.modules.access);
  }

  function renderManagers() {
    var managers = matrixEmployees.filter(isManager);
    var others = matrixEmployees.filter(function (employee) { return !isManager(employee); });

    if (managerChip) {
      managerChip.innerHTML = '<i class="bi bi-shield-lock"></i> <strong>' + managers.length +
        '</strong> access manager' + (managers.length === 1 ? '' : 's');
    }

    if (managerList) {
      if (!managers.length) {
        managerList.innerHTML = '<p class="ps-muted" style="font-size:0.82rem;margin:0;">' +
          'Nobody can manage access yet. Grant it below, or give an employee the ' +
          '<em>Manage access</em> switch in the matrix.</p>';
      } else {
        managerList.innerHTML = managers.map(function (employee) {
          return '<div class="ps-manager">' +
            '<label class="ps-switch" title="Manage access for ' + escapeHtml(employee.name) + '">' +
            '<input type="checkbox" checked data-employee="' + employee.id + '" data-module="access" ' +
            'aria-label="Manage access for ' + escapeHtml(employee.name) + '">' +
            '<span class="ps-switch-track"></span>' +
            '</label>' +
            '<span class="ps-manager-details">' +
            '<span class="ps-manager-name">' + escapeHtml(employee.name) + '</span>' +
            '<span class="ps-manager-role">' + escapeHtml(employee.role || 'Employee') + '</span>' +
            '</span></div>';
        }).join('');
      }
    }

    if (grantSelect) {
      grantSelect.innerHTML = '<option value="">Grant access management to…</option>' +
        others.map(function (employee) {
          return '<option value="' + employee.id + '">' + escapeHtml(employee.name) +
            (employee.role ? ' — ' + escapeHtml(employee.role) : '') + '</option>';
        }).join('');
      grantSelect.disabled = !others.length;
    }
    if (grantButton) grantButton.disabled = !others.length;
  }

  function applyToggle(employeeId, module, enabled, checkbox) {
    var wrapper = checkbox ? checkbox.closest('.ps-switch') : null;
    var before = !enabled;
    var moduleTitle = module;
    matrixModules.forEach(function (entry) {
      if (entry.key === module) moduleTitle = entry.full_title || entry.title;
    });
    if (wrapper) wrapper.classList.add('is-busy');

    return fetch('/api/access/toggle', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ employee_id: parseInt(employeeId, 10), module: module, enabled: enabled })
    })
      .then(function (response) { return response.json(); })
      .then(function (result) {
        if (!result.success) {
          if (checkbox) checkbox.checked = before;
          showAlert(result.message || 'Could not update portal access.', 'error');
          return false;
        }
        var employee = matrixEmployees.filter(function (entry) {
          return String(entry.id) === String(employeeId);
        })[0];
        if (employee) {
          employee.modules = employee.modules || {};
          employee.modules[module] = enabled;
        }
        renderMatrix(matrixEmployees);
        renderManagers();
        showAlert(moduleTitle + (enabled ? ' enabled' : ' disabled') + ' for ' +
          ((employee && employee.name) || 'this employee') +
          '. The change applies from their next sign in.', 'ok');
        return true;
      })
      .catch(function () {
        if (checkbox) checkbox.checked = before;
        showAlert('Could not update portal access.', 'error');
        return false;
      })
      .then(function (value) {
        if (wrapper) wrapper.classList.remove('is-busy');
        return value;
      });
  }

  document.addEventListener('change', function (event) {
    var checkbox = event.target.closest('input[data-module]');
    if (!checkbox) return;
    applyToggle(checkbox.getAttribute('data-employee'), checkbox.getAttribute('data-module'),
                checkbox.checked, checkbox);
  });

  if (grantButton && grantSelect) {
    grantButton.addEventListener('click', function () {
      var employeeId = grantSelect.value;
      if (!employeeId) {
        showAlert('Choose an employee to grant access management to.', 'error');
        return;
      }
      applyToggle(employeeId, 'access', true, null);
    });
  }

  if (matrixBody) {
    var refresh = document.getElementById('refreshAccessMatrix');
    if (refresh) refresh.addEventListener('click', loadMatrix);
    loadMatrix();
  }
})();
