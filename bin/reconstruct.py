import json
import re
import tomllib

with open("conduwuit.toml", "rb") as f:
    config = tomllib.load(f)

# Manually insert the dropped keys from the scrollback memory mapping
config["global"]["proxy"] = "none"
config["global"]["trusted_servers"] = [
    "matrix.org",
    "tchncs.de",
    "mozilla.org",
    "unredacted.org",
]
config["global"]["query_trusted_key_servers_first"] = False
config["global"]["query_trusted_key_servers_first_on_join"] = True
config["global"]["only_query_trusted_key_servers"] = False
config["global"]["trusted_server_batch_size"] = 1024
config["global"]["log"] = "debug"
config["global"]["log_colors"] = True
config["global"]["log_span_events"] = "none"
config["global"]["log_filter_regex"] = True
config["global"]["log_thread_ids"] = False
config["global"]["log_to_journald"] = False
config["global"]["openid_token_ttl"] = 3600
config["global"]["login_via_existing_session"] = True
config["global"]["login_token_ttl"] = 120000
config["global"]["turn_username"] = False
config["global"]["turn_password"] = False
config["global"]["turn_uris"] = [
    "turn:turn.nutra.tk:3478?transport=udp",
    "turn:turn.nutra.tk:3478?transport=tcp",
    "turns:turn.nutra.tk:5349?transport=udp",
    "turns:turn.nutra.tk:5349?transport=tcp",
]
config["global"]["turn_secret"] = (
    "cce010efa85189b63ce2b2bf530049e8ac965275366e06f4a739e419e1837424"
)
config["global"]["turn_ttl"] = 86400
config["global"]["auto_join_rooms"] = ["#nutra:nutra.tk", "#general:nutra.tk"]
config["global"]["auto_deactivate_banned_room_attempts"] = False
config["global"]["rocksdb_log_level"] = "error"
config["global"]["rocksdb_log_stderr"] = False
config["global"]["rocksdb_max_log_file_size"] = 4194304
config["global"]["rocksdb_log_time_to_roll"] = 0
config["global"]["rocksdb_optimize_for_spinning_disks"] = False
config["global"]["rocksdb_direct_io"] = True
config["global"]["rocksdb_parallelism_threads"] = 2
config["global"]["rocksdb_max_log_files"] = 3
config["global"]["rocksdb_compression_algo"] = "zstd"
config["global"]["rocksdb_compression_level"] = 32767
config["global"]["rocksdb_bottommost_compression_level"] = 32767
config["global"]["rocksdb_bottommost_compression"] = True
config["global"]["rocksdb_wal_compression"] = "zstd"
config["global"]["rocksdb_recovery_mode"] = 1
config["global"]["rocksdb_paranoid_file_checks"] = False
config["global"]["rocksdb_checksums"] = True
config["global"]["rocksdb_atomic_flush"] = False
config["global"]["rocksdb_repair"] = False
config["global"]["rocksdb_read_only"] = False
config["global"]["rocksdb_secondary"] = False
config["global"]["rocksdb_compaction_prio_idle"] = False
config["global"]["rocksdb_compaction_ioprio_idle"] = True
config["global"]["rocksdb_compaction"] = True
config["global"]["rocksdb_stats_level"] = 1
config["global"]["allow_local_presence"] = True
config["global"]["allow_incoming_presence"] = True
config["global"]["allow_outgoing_presence"] = True
config["global"]["presence_idle_timeout_s"] = 300
config["global"]["presence_offline_timeout_s"] = 1800
config["global"]["presence_timeout_remote_users"] = True
config["global"]["allow_local_read_receipts"] = True
config["global"]["allow_incoming_read_receipts"] = True
config["global"]["allow_outgoing_read_receipts"] = True
config["global"]["allow_local_typing"] = True
config["global"]["allow_outgoing_typing"] = True
config["global"]["allow_incoming_typing"] = True
config["global"]["typing_federation_timeout_s"] = 30
config["global"]["typing_client_timeout_min_s"] = 15
config["global"]["typing_client_timeout_max_s"] = 45
config["global"]["zstd_compression"] = False
config["global"]["gzip_compression"] = False
config["global"]["brotli_compression"] = False
config["global"]["allow_guest_registration"] = False
config["global"]["log_guest_registrations"] = False
config["global"]["allow_guests_auto_join_rooms"] = False
config["global"]["allow_legacy_media"] = True
config["global"]["media_startup_check"] = True
config["global"]["media_compat_file_link"] = False
config["global"]["prune_missing_media"] = False
config["global"]["forbidden_remote_server_names"] = ["im.kde.org", "kde.org"]
config["global"]["allowed_remote_server_names"] = []
config["global"]["prevent_media_downloads_from"] = []
config["global"]["forbidden_remote_room_directory_server_names"] = []
config["global"]["ignore_messages_from_server_names"] = []
config["global"]["send_messages_from_ignored_users_to_client"] = False
config["global"]["url_preview_domain_explicit_allowlist"] = ["*"]
config["global"]["url_preview_check_root_domain"] = False
config["global"]["url_preview_user_agent"] = (
    "continuwuity/<version> (bot; +https://continuwuity.org)"
)
config["global"]["forbidden_alias_names"] = []
config["global"]["forbidden_usernames"] = []
config["global"]["startup_netburst"] = True
config["global"]["startup_netburst_keep"] = 50
config["global"]["block_non_admin_invites"] = False
config["global"]["enable_msc4284_policy_servers"] = True
config["global"]["policy_server_check_own_events"] = True
config["global"]["admin_escape_commands"] = True
config["global"]["admin_console_automatic"] = False
config["global"]["admin_execute"] = []
config["global"]["admin_execute_errors_ignore"] = False
config["global"]["admin_signal_execute"] = []
config["global"]["admin_log_capture"] = "info"
config["global"]["admin_room_tag"] = "m.server_notice"
config["global"]["admins_list"] = []
config["global"]["admins_from_room"] = True
config["global"]["sentry"] = False
config["global"]["sentry_send_server_name"] = False
config["global"]["sentry_traces_sample_rate"] = 0.15
config["global"]["sentry_attach_stacktrace"] = False
config["global"]["sentry_send_panic"] = True
config["global"]["sentry_send_error"] = True
config["global"]["sentry_filter"] = "info"
config["global"]["tokio_console"] = False
config["global"]["test"] = False
config["global"]["admin_room_notices"] = True
config["global"]["db_pool_affinity"] = True
config["global"]["db_pool_workers"] = 32
config["global"]["db_pool_workers_limit"] = 64
config["global"]["db_pool_queue_mult"] = 4
config["global"]["stream_width_default"] = 32
config["global"]["stream_width_scale"] = 1.0
config["global"]["stream_amplification"] = 1024
config["global"]["sender_workers"] = 3
config["global"]["listening"] = True
config["global"]["config_reload_signal"] = True

config["global"]["well_known"] = {}
config["global"]["well_known"]["support_role"] = "m.role.admin"
config["global"]["well_known"]["support_email"] = "shane@nutra.tk"
config["global"]["well_known"]["rtc_focus_server_urls"] = []

config["global"]["blurhashing"] = {}
config["global"]["blurhashing"]["components_x"] = 4
config["global"]["blurhashing"]["components_y"] = 3
config["global"]["blurhashing"]["blurhash_max_raw_size"] = 33554432

config["global"]["ldap"] = {}
config["global"]["ldap"]["enable"] = False
config["global"]["ldap"]["ldap_only"] = False
config["global"]["ldap"]["filter"] = "(objectClass=*)"
config["global"]["ldap"]["uid_attribute"] = "uid"
config["global"]["ldap"]["name_attribute"] = "givenName"

config["global"]["experimental_features"] = {}
config["global"]["experimental_features"]["msc3266_enabled"] = True
config["global"]["experimental_features"]["msc4222_enabled"] = True

with open("conduwuit-example.toml", "r") as f:
    example_lines = f.readlines()


def format_value(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    elif isinstance(v, str):
        return json.dumps(v, ensure_ascii=False)
    elif isinstance(v, list):
        if len(v) == 0:
            return "[]"
        contents = ", ".join(
            json.dumps(i, ensure_ascii=False) if isinstance(i, str) else str(i)
            for i in v
        )
        return f"[{contents}]"
    elif isinstance(v, (int, float)):
        return str(v)
    elif isinstance(v, dict):
        return json.dumps(v, ensure_ascii=False)
    else:
        return str(v)


merged_lines = []
current_sect = "global"

i = 0
while i < len(example_lines):
    line = example_lines[i]
    sect_match = re.match(r"^#?\[([a-zA-Z0-9_\.]+)\]", line)
    if sect_match:
        sect = sect_match.group(1)
        merged_lines.append(f"[{sect}]\n")
        current_sect = sect
        i += 1
        continue

    kv_match = re.match(r"^#?\s*([a-zA-Z0-9_-]+)\s*=", line)
    if kv_match:
        key = kv_match.group(1)
        parts = current_sect.split(".")
        curr = config
        has_val = True
        for p in parts:
            if isinstance(curr, dict) and p in curr:
                curr = curr[p]
            else:
                has_val = False
                break

        val = None
        if has_val and isinstance(curr, dict) and key in curr:
            val = curr[key]
        else:
            has_val = False

        if has_val:
            merged_lines.append(f"{key} = {format_value(val)}\n")
        else:
            merged_lines.append(line)
        i += 1
        continue

    merged_lines.append(line)
    i += 1

with open("conduwuit.toml", "w") as f:
    f.writelines(merged_lines)
