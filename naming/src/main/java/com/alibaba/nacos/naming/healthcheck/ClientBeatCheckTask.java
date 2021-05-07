/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.healthcheck;

import com.alibaba.nacos.common.utils.IPUtil;
import com.alibaba.nacos.common.http.Callback;
import com.alibaba.nacos.common.model.RestResult;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.healthcheck.events.InstanceHeartbeatTimeoutEvent;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.HttpClient;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NamingProxy;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.push.PushService;
import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

/**
 * 检查并更新临时实例的状态，如果它们已经过期，则将其删除
 */
public class ClientBeatCheckTask implements Runnable {

    private Service service;

    public ClientBeatCheckTask(Service service) {
        this.service = service;
    }

    @JsonIgnore
    public PushService getPushService() {
        return ApplicationUtils.getBean(PushService.class);
    }

    @JsonIgnore
    public DistroMapper getDistroMapper() {
        return ApplicationUtils.getBean(DistroMapper.class);
    }

    public GlobalConfig getGlobalConfig() {
        return ApplicationUtils.getBean(GlobalConfig.class);
    }

    public SwitchDomain getSwitchDomain() {
        return ApplicationUtils.getBean(SwitchDomain.class);
    }

    public String taskKey() {
        return KeyBuilder.buildServiceMetaKey(service.getNamespaceId(), service.getName());
    }

    @Override
    public void run() {
        try {
            if (!getDistroMapper().responsible(service.getName())) {
                return;
            }

            if (!getSwitchDomain().isHealthCheckEnabled()) {
                return;
            }

            List<Instance> instances = service.allIPs(true);

            // 首先设置实例的健康状况
            for (Instance instance : instances) {
                // 如果当前时间和实例最后一次发送心跳的时间的差值大于了设置的PreservedMetadataKeys.HEART_BEAT_TIMEOUT时间(默认15秒)
                // 那么将该实例设置为非健康状态，同时发布InstanceHeartbeatTimeoutEvent和ServiceChangeEvent事件
                if (System.currentTimeMillis() - instance.getLastBeat() > instance.getInstanceHeartBeatTimeOut()) {
                    if (!instance.isMarked()) {
                        if (instance.isHealthy()) {
                            instance.setHealthy(false);
                            Loggers.EVT_LOG
                                .info("{POS} {IP-DISABLED} valid: {}:{}@{}@{}, region: {}, msg: client timeout after {}, last beat: {}",
                                    instance.getIp(), instance.getPort(), instance.getClusterName(),
                                    service.getName(), UtilsAndCommons.LOCALHOST_SITE,
                                    instance.getInstanceHeartBeatTimeOut(), instance.getLastBeat());
                            // 发布ServiceChangeEvent事件，该事件的监听器是PushService
                            this.getPushService().serviceChanged(service);
                            // 发布InstanceHeartbeatTimeoutEvent事件
                            ApplicationUtils.publishEvent(new InstanceHeartbeatTimeoutEvent(this, instance));
                        }
                    }
                }
            }

            if (!getGlobalConfig().isExpireInstance()) {
                return;
            }

            // 然后删除过时的实例
            for (Instance instance : instances) {
                if (instance.isMarked()) {
                    continue;
                }

                // 如果当前时间和实例最后一次发送心跳的时间的差值大于了设置的PreservedMetadataKeys.IP_DELETE_TIMEOUT时间(默认30秒)，那么删除该实例
                if (System.currentTimeMillis() - instance.getLastBeat() > instance.getIpDeleteTimeout()) {
                    // delete instance
                    Loggers.SRV_LOG.info("[AUTO-DELETE-IP] service: {}, ip: {}", service.getName(), JacksonUtils.toJson(instance));
                    deleteIp(instance);
                }
            }
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("Exception while processing client beat time out.", e);
        }
    }

    /**
     * 删除实例，调用的接口为{@link com.alibaba.nacos.naming.controllers.InstanceController#deregister(HttpServletRequest)}
     *
     * @param instance
     */
    private void deleteIp(Instance instance) {
        try {
            NamingProxy.Request request = NamingProxy.Request.newRequest();
            request.appendParam("ip", instance.getIp()).appendParam("port", String.valueOf(instance.getPort()))
                .appendParam("ephemeral", "true").appendParam("clusterName", instance.getClusterName())
                .appendParam("serviceName", service.getName()).appendParam("namespaceId", service.getNamespaceId());

            String url = "http://" + IPUtil.localHostIP() + IPUtil.IP_PORT_SPLITER + EnvUtil.getPort() + EnvUtil.getContextPath()
                + UtilsAndCommons.NACOS_NAMING_CONTEXT + "/instance?" + request.toUrl();

            // 异步删除实例
            HttpClient.asyncHttpDelete(url, null, null, new Callback<String>() {
                @Override
                public void onReceive(RestResult<String> result) {
                    if (!result.ok()) {
                        Loggers.SRV_LOG
                            .error("[IP-DEAD] failed to delete ip automatically, ip: {}, caused {}, resp code: {}",
                                instance.toJson(), result.getMessage(), result.getCode());
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    Loggers.SRV_LOG
                        .error("[IP-DEAD] failed to delete ip automatically, ip: {}, error: {}", instance.toJson(),
                            throwable);
                }

                @Override
                public void onCancel() {

                }
            });

        } catch (Exception e) {
            Loggers.SRV_LOG
                .error("[IP-DEAD] failed to delete ip automatically, ip: {}, error: {}", instance.toJson(), e);
        }
    }
}
