{{- define "nifi.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "nifi.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "nifi.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "nifi.labels" -}}
app.kubernetes.io/name: {{ include "nifi.name" . }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
{{- end -}}

{{- define "nifi.serviceName" -}}
{{- default (include "nifi.fullname" .) .Values.nifi.service.name -}}
{{- end -}}

{{- define "nifi.namespace" -}}
{{- default .Release.Namespace .Values.nifi.namespace -}}
{{- end -}}

{{- define "nifi.imageRepository" -}}
{{- $repository := .Values.nifi.image.repository -}}
{{- if and .Values.global.imageRegistry $repository (not (contains "." $repository)) -}}
{{- printf "%s/%s" (trimSuffix "/" .Values.global.imageRegistry) (trimPrefix "/" $repository) -}}
{{- else -}}
{{- $repository -}}
{{- end -}}
{{- end -}}

{{- define "nifi.imagePullSecrets" -}}
{{- if .Values.global.imagePullSecrets -}}
{{- toYaml .Values.global.imagePullSecrets -}}
{{- end -}}
{{- end -}}

{{- define "nifi.mcpFullname" -}}
{{- printf "%s-mcp" (include "nifi.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "nifi.mcpServiceName" -}}
{{- include "nifi.mcpFullname" . -}}
{{- end -}}

{{- define "nifi.mcpSelectorLabels" -}}
app.kubernetes.io/name: {{ include "nifi.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: mcp
{{- end -}}

{{- define "nifi.mcpRepository" -}}
{{- $repo := .Values.mcp.image.repository -}}
{{- if and $repo .Values.global.imageRegistry (not (contains "." $repo)) -}}
{{- printf "%s/%s" (trimSuffix "/" .Values.global.imageRegistry) (trimPrefix "/" $repo) -}}
{{- else -}}
{{- $repo -}}
{{- end -}}
{{- end -}}

{{- define "nifi.mcpConfigMapName" -}}
{{- $name := default "" .Values.mcp.config.name -}}
{{- if $name -}}
{{- $name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-config" (include "nifi.mcpFullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "nifi.mcpConfigContent" -}}
{{- $cfg := .Values.mcp.config | default dict -}}
{{- $serviceName := include "nifi.serviceName" . -}}
{{- $namespace := include "nifi.namespace" . -}}
{{- $port := int (.Values.nifi.service.port | default 8080) -}}
{{- $scheme := "http" -}}
{{- $defaultUrl := printf "%s://%s.%s.svc:%d/nifi-api" $scheme $serviceName $namespace $port -}}
{{- $defaultUser := default "" .Values.nifi.auth.singleUser.username -}}
{{- $defaultPass := default "" .Values.nifi.auth.singleUser.password -}}
{{- $clusterId := include "nifi.fullname" . -}}
{{- $clusterDisplayName := printf "NiFi Cluster (%s)" $clusterId -}}
{{- $defaultServer := dict "id" $clusterId "name" $clusterDisplayName "url" $defaultUrl "username" $defaultUser "password" $defaultPass "tlsVerify" false -}}
{{- $servers := $cfg.servers | default (list $defaultServer) -}}
{{- if or (not $servers) (eq (len $servers) 0) -}}
{{- $servers = list $defaultServer -}}
{{- end -}}
nifi:
  servers:
{{ range $index, $server := $servers }}
{{- $ordinal := add $index 1 }}
{{- $idDefault := ternary $clusterId (printf "%s-%d" $clusterId $ordinal) (eq $index 0) }}
{{- $nameDefault := ternary $clusterDisplayName (printf "%s #%d" $clusterDisplayName $ordinal) (eq $index 0) }}
{{- $urlVal := default $defaultServer.url $server.url }}
{{- $url := tpl $urlVal $ }}
{{- $userVal := default $defaultServer.username $server.username }}
{{- $user := tpl $userVal $ }}
{{- $passVal := default $defaultServer.password $server.password }}
{{- $pass := tpl $passVal $ }}
{{- $tlsVal := default (default $defaultServer.tlsVerify $server.tlsVerify) $server.tls_verify }}
{{- $rendered := dict "id" (default $idDefault $server.id) "name" (default $nameDefault $server.name) "url" $url "username" $user "password" $pass "tls_verify" $tlsVal }}
{{- toYaml (list $rendered) | trimSuffix "\n" | indent 4 }}
{{- end }}
{{- with $cfg.extra }}

{{ toYaml . }}
{{- end }}
{{- end -}}
