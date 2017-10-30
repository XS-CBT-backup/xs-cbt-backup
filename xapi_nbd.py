import time


def _has_vlan_pif(session, network):
    """
    Returns true if there is a PIF on this network which is the master PIF of a
    VLAN.
    """
    for pif in session.xenapi.network.get_PIFs(network):
        if session.xenapi.PIF.get_VLAN_master_of(pif) != "OpaqueRef:NULL":
            return True
    return False


def _enable_nbd(session, networks, nbd_purpose, skip_vlan_networks):
    for network in networks:
        if not (skip_vlan_networks and _has_vlan_pif(session, network)):
            session.xenapi.network.add_purpose(network, nbd_purpose)


def enable_nbd_on_all_networks(session, use_tls=True, skip_vlan_networks=True):
    nbd_purpose = "nbd" if use_tls else "insecure_nbd"
    networks = session.xenapi.network.get_all()
    _enable_nbd(
        session=session,
        networks=networks,
        nbd_purpose=nbd_purpose,
        skip_vlan_networks=skip_vlan_networks)
    # wait for a bit for the changes to take effect
    # We do rate limiting with a 5s delay, so sometimes the update
    # takes at least 5 seconds
    time.sleep(7)


def auto_enable_nbd(session, use_tls=True, skip_vlan_networks=True):
    """
    If there is a network on which NBD is already enabled,
    this function does nothing. Otherwise, it enables NBD on
    all networks.
    """
    (nbd_purpose, conflicting_nbd_purpose) = (
        "nbd", "insecure_nbd") if use_tls else ("insecure_nbd", "nbd")
    networks = session.xenapi.network.get_all()
    for network in networks:
        purpose = session.xenapi.network.get_purpose(network)
        if nbd_purpose in purpose:
            return
        if conflicting_nbd_purpose in purpose:
            session.xenapi.network.remove_purpose(network,
                                                  conflicting_nbd_purpose)
    _enable_nbd(
        session=session,
        networks=networks,
        nbd_purpose=nbd_purpose,
        skip_vlan_networks=skip_vlan_networks)
    # wait for a bit for the changes to take effect
    # We do rate limiting with a 5s delay, so sometimes the update
    # takes at least 5 seconds
    time.sleep(7)
