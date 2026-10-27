#!/bin/sh
set -eu

PD_HOST=${PD_HOST:?set PD_HOST to the PD host}
SSH_USER=${SSH_USER:-why}
REMOTE_VALSD=${REMOTE_VALSD:-/home/user/vals/target/release/valsd}
PD_ADMIN_SOCKET=${PD_ADMIN_SOCKET:-/home/user/vals/run/pd.sock}
ZONE=${ZONE:?set numeric dedicated test zone id}
READ_QUORUM=${READ_QUORUM:-2}
WRITE_QUORUM=${WRITE_QUORUM:-2}
SSH_TIMEOUT=${SSH_TIMEOUT:-20}

zones=$(timeout "${SSH_TIMEOUT}s" ssh -o BatchMode=yes -o ConnectTimeout="$SSH_TIMEOUT" "$SSH_USER@$PD_HOST" \
  "'$REMOTE_VALSD' admin -s '$PD_ADMIN_SOCKET' zones")
line=$(printf '%s\n' "$zones" | awk -v zone="$ZONE" '$1 == zone && $3 == 3 && $4 == "sorted" { print; found=1 } END { exit !found }')
name=$(printf '%s\n' "$line" | awk '{print $2}')
printf 'ZONE_STATUS zone=%s name=%s sorted=1 rf=3 declared_w=%s declared_r=%s\n' "$ZONE" "$name" "$WRITE_QUORUM" "$READ_QUORUM"
printf 'ZONE_STATUS_NOTE declared_w/declared_r are operator-declared intent; the executed quorums are the driver majority reported as quorum=N in the PROFILECHECK PASS line (Durability::Majority writes, R=quorum reads)\n'
[ "$WRITE_QUORUM" -eq 2 ] && [ "$READ_QUORUM" -eq 2 ]
