# VMware Tools is not running - Alert Documentation

## Alert Description
**Name:** VMware Tools is not running  
**Trigger Expression:** `last(/VMware Guest/vmware.vm.tools[{$VMWARE.URL},{$VMWARE.VM.UUID},status]) = 1`  
**Description:** This alert monitors the status of the `open-vm-tools` (or `vmware-tools`) service within the guest operating system. It relies on the hypervisor (ESXi/vCenter) reporting the guest tools status.

## Side Effects of Manual Triggering
If you manually stop the VMware Tools service to test this alert, the following side effects will occur:

1.  **Graceful Shutdown Loss:** You will be unable to issue a "Shut down Guest" command from the vSphere/vCenter console. You would be forced to use "Power Off" (hard cut) if you cannot access the VM via SSH.
2.  **Heartbeat Loss:** The hypervisor will report the "Guest Heartbeat" as "Not Running" or "Gray".
3.  **Time Sync Issues:** If the VM relies on the host for time synchronization (instead of NTP), the system time may drift.
4.  **Performance Data Loss:** Some guest-level performance metrics collected by the hypervisor (like precise memory usage) may become unavailable or inaccurate.

## How to Manually Trigger (Test Procedure)

To simulate this error condition on the VM, you can stop the service manually.

### 1. Check current status
Verify the service is running:
```bash
systemctl status open-vm-tools
```

### 2. Trigger the Alert (Stop Service)
Stop the service to break the heartbeat connection to the hypervisor:
```bash
sudo systemctl stop open-vm-tools
```
*Wait for Zabbix to poll the VMware interface (this may take a few minutes).*

### 3. Restore Normal Operation (Fix)
Start the service again to resolve the alert:
```bash
sudo systemctl start open-vm-tools
```
Verify it is active:
```bash
sudo systemctl start open-vm-tools
```
Verify it is active:
```bash
systemctl is-active open-vm-tools
```

## Troubleshooting: Host Disappears/Disabled
**Problem:** When you stop the service, the Zabbix Host becomes "Disabled" with the message *"The host is not discovered anymore and has been disabled"*.  
**Reason:** Your host is likely added via **Low Level Discovery (LLD)**. The Discovery Rule is configured to only discover VMs that are "Running" or reporting tools status. When tools stop, the VM no longer matches the discovery filter, so Zabbix disables it.

**Solution (How to prevent this):**
1.  In Zabbix, go to **Data collection** -> **Discovery** (or the Template's Discovery Rules).
2.  Find the rule named **"Discover VMware VMs"** (or similar).
3.  Look for the setting **"Keep lost resources period"** or **"Disable lost resources"**.
    *   If "Disable lost resources" is set to "Immediately", change it to a longer period (e.g., `1h`) or set it to "Never".
4.  Alternatively, for this test, you can manually **Enable** the host again in the Zabbix Hosts list after it gets disabled, though LLD might disable it again on the next cycle.

### How to Find the Discovery Rule
The Discovery Rule that disables your VM is **NOT** on the VM host itself. It is on the "Parent" host that monitors your VMware environment (e.g., vCenter or ESXi host).

1.  **Identify the Parent Host:**
    *   Find the Zabbix Host that represents your vCenter or ESXi server (it usually has the `VMware` template attached).
    *   This is the host where you originally configured the `{$VMWARE.URL}`, `{$VMWARE.USERNAME}`, etc.

2.  **Go to Discovery Rules:**
    *   Go to **Data collection** -> **Hosts**.
    *   Find that *Parent Host*.
    *   Click on **Discovery** in that host's row.

3.  **Edit the Rule:**
    *   Look for a rule named **"Discover VMware VMs"** (or similar).
    *   Click on it to edit.
    *   Scroll down to the **"Lost resources"** section.
    *   Set **"Delete lost resources"** to **"Never"**.
    *   Set **"Disable lost resources"** to **"Never"**.
    *   *Note: Zabbix requires "Disable" time to be shorter than "Delete" time, or both to be Never.*
