package com.alibaba.nacos.trace.logging;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.common.trace.event.TraceEvent;
import com.alibaba.nacos.common.trace.event.naming.DeregisterInstanceTraceEvent;
import com.alibaba.nacos.common.trace.event.naming.RegisterInstanceTraceEvent;
import com.alibaba.nacos.common.trace.event.naming.UpdateInstanceTraceEvent;
import com.alibaba.nacos.plugin.trace.spi.NacosTraceSubscriber;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class NacosLoggingNamingTraceSubscriber implements NacosTraceSubscriber {

    private static final Logger LOGGER = LoggerFactory.getLogger(NacosLoggingNamingTraceSubscriber.class);
    private static final String NAME = "namingLogging";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    // 反射拿到的 JdbcTemplate 和 update 方法
    private static Object jdbcTemplate;
    private static Method updateMethod;

    static {
        try {
            // 1. 获取 Nacos 核心类加载器
            ClassLoader nacosCoreLoader = Thread.currentThread().getContextClassLoader();

            // 2. 反射加载 DynamicDataSource 类
            Class<?> dynamicDsClass = nacosCoreLoader.loadClass("com.alibaba.nacos.persistence.datasource.DynamicDataSource");

            // 3. 调用 DynamicDataSource.getInstance()
            Method getInstanceMethod = dynamicDsClass.getDeclaredMethod("getInstance");
            Object dynamicDsInstance = getInstanceMethod.invoke(null);

            // 4. 调用 getDataSource()
            Method getDataSourceMethod = dynamicDsClass.getDeclaredMethod("getDataSource");
            Object dataSourceService = getDataSourceMethod.invoke(dynamicDsInstance);

            // 5. 获取 DataSourceService 类
            Class<?> dsServiceClass = nacosCoreLoader.loadClass("com.alibaba.nacos.persistence.datasource.DataSourceService");

            // 6.调用 getJdbcTemplate()
            Method getJdbcTemplateMethod = dsServiceClass.getDeclaredMethod("getJdbcTemplate");
            jdbcTemplate = getJdbcTemplateMethod.invoke(dataSourceService);

            // 7. 获取 update 方法
            Class<?> jtClass = jdbcTemplate.getClass();
            updateMethod = jtClass.getMethod("update", String.class, Object[].class);

            LOGGER.info("成功获取 JdbcTemplate 实例：" + jdbcTemplate);

        } catch (Exception e) {
            LOGGER.error("[Nacos get JDBC ERROR]", e);
        }
    }

    private final NamingService namingService;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

    public NacosLoggingNamingTraceSubscriber() {
        NamingService tmp = null;
        try {
            Properties properties = new Properties();
            properties.setProperty(PropertyKeyConst.SERVER_ADDR, "localhost:8848");
            properties.setProperty(PropertyKeyConst.NAMESPACE, "public");
            tmp = NacosFactory.createNamingService(properties);
            LOGGER.info("[NamingTraceSubscriber] NamingService initialized successfully");
        } catch (NacosException e) {
            LOGGER.error("[NamingTraceSubscriber] Failed to initialize NamingService", e);
        }
        this.namingService = tmp;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void onEvent(TraceEvent event) {
        if (event instanceof RegisterInstanceTraceEvent) {
            loggingRegisterInstance((RegisterInstanceTraceEvent) event);
        } else if (event instanceof DeregisterInstanceTraceEvent) {
            loggingDeregisterInstance((DeregisterInstanceTraceEvent) event);
        } else if (event instanceof UpdateInstanceTraceEvent) {
            loggingUpdatingInstance((UpdateInstanceTraceEvent) event);
        }
    }

    private void loggingRegisterInstance(RegisterInstanceTraceEvent event) {
        LOGGER.info("[loggingRegisterInstance] clientIp:{}, instanceIp:{}, instancePort:{}, type:{}, eventTime:{}, namespace:{}, groupName:{}, name:{} register a new instance {} by {}", event.getClientIp(), event.getInstanceIp(), event.getInstancePort(), event.getType(), event.getEventTime(), event.getNamespace(), event.getGroup(), event.getName(), event.toInetAddr(), event.isRpc() ? "gRPC" : "HTTP");
        scheduler.schedule(() -> {
            try {
                List<Instance> instances = namingService.getAllInstances(event.getName(), event.getGroup());
                instances.forEach(instance -> LOGGER.info("[loggingRegisterInstance] instance: {}", instance));

                // 匹配目标实例
                Instance matchedInstance = instances.stream().filter(instance -> instance.getIp().equals(event.getInstanceIp()) && instance.getPort() == event.getInstancePort()).findFirst().orElse(null);

                if (matchedInstance == null) {
                    LOGGER.info("[loggingRegisterInstance] cannot find matched instance");
                } else if (jdbcTemplate == null || updateMethod == null) {
                    LOGGER.info("[loggingRegisterInstance] JDBC not initialized");
                } else {
                    String sql = "INSERT IGNORE INTO nacos_instance (" + "id, instance_id, ip, port, weight, healthy, enabled, ephemeral, cluster_name, service_name, metadata, gmt_create, gmt_modified" + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)";

                    Object[] params = new Object[]{matchedInstance.getPort() + "@" + matchedInstance.getIp(),        // id
                            matchedInstance.getInstanceId(),                                  // instance_id
                            matchedInstance.getIp(),                                          // ip
                            matchedInstance.getPort(),                                        // port
                            matchedInstance.getWeight(),                                      // weight
                            matchedInstance.isHealthy() ? 1 : 0,                              // healthy
                            matchedInstance.isEnabled() ? 1 : 0,                              // enabled
                            matchedInstance.isEphemeral() ? 1 : 0,                            // ephemeral
                            matchedInstance.getClusterName(),                                  // cluster_name
                            matchedInstance.getServiceName(),                                  // service_name
                            OBJECT_MAPPER.writeValueAsString(matchedInstance.getMetadata())   // metadata
                    };

                    Object result = updateMethod.invoke(jdbcTemplate, sql, params);
                    LOGGER.info("[loggingRegisterInstance] 插入数据库受影响行数: {}", result);

                }

            } catch (NacosException | JsonProcessingException e) {
                LOGGER.error("[loggingRegisterInstance] exception:", e);
            } catch (Exception e) {
                LOGGER.error("[loggingRegisterInstance] JDBC invoke exception:", e);
            }
        }, 1000, TimeUnit.MILLISECONDS);

    }

    private void loggingDeregisterInstance(DeregisterInstanceTraceEvent event) {
        LOGGER.info("[loggingDeregisterInstance] namespaceId:{}, groupName:{}, serviceName:{} deregister an instance {} for reason:{}", event.getNamespace(), event.getGroup(), event.getName(), event.toInetAddr(), event.getReason());

        try {
            // 从 NamingService 获取当前服务的所有实例（可能还包含其他节点）
            List<Instance> instances = namingService.getAllInstances(event.getName(), event.getGroup());
            String targetIp = event.getInstanceIp();
            int targetPort = event.getInstancePort();


            if (jdbcTemplate != null && updateMethod != null) {
                // 删除逻辑：根据 端口@IP 删除对应记录
                String sql = "DELETE FROM nacos_instance WHERE id = ? ";
                Object[] params = new Object[]{targetPort + "@" + targetIp};

                Object result = updateMethod.invoke(jdbcTemplate, sql, params);
                LOGGER.info("[loggingDeregisterInstance] 删除数据库受影响行数: {}", result);

            } else {
                LOGGER.warn("[loggingDeregisterInstance] JDBC not initialized, skip deletion.");
            }

        } catch (NacosException e) {
            LOGGER.error("[loggingDeregisterInstance] 获取实例列表异常:", e);
        } catch (Exception e) {
            LOGGER.error("[loggingDeregisterInstance] JDBC 删除异常:", e);
        }
    }

    private void loggingUpdatingInstance(UpdateInstanceTraceEvent event) {
        LOGGER.info("[loggingUpdateInstance] namespaceId:{}, groupName:{}, serviceName:{} updating instance {}", event.getNamespace(), event.getGroup(), event.getName(), event.toInetAddr());

        scheduler.schedule(() -> {
            try {
                // 获取该服务的所有实例
                List<Instance> instances = namingService.getAllInstances(event.getName(), event.getGroup());
                instances.forEach(instance -> LOGGER.info("[loggingUpdateInstance] instance: {}", instance));

                // 根据 IP + Port 查找目标实例
                Instance matchedInstance = instances.stream().filter(instance -> instance.getIp().equals(event.getInstanceIp()) && instance.getPort() == event.getInstancePort()).findFirst().orElse(null);

                if (matchedInstance == null) {
                    LOGGER.info("[loggingUpdateInstance] cannot find matched instance");
                    return;
                }

                if (jdbcTemplate == null || updateMethod == null) {
                    LOGGER.warn("[loggingUpdateInstance] JDBC not initialized");
                    return;
                }

                // 构建 SQL
                String sql = "UPDATE nacos_instance SET " + "weight = ?, healthy = ?, enabled = ?, ephemeral = ?, " + "cluster_name = ?, service_name = ?, metadata = ?, " + "gmt_modified = CURRENT_TIMESTAMP " + "WHERE id = ?";

                Object[] params = new Object[]{matchedInstance.getWeight(), matchedInstance.isHealthy() ? 1 : 0, matchedInstance.isEnabled() ? 1 : 0, matchedInstance.isEphemeral() ? 1 : 0, matchedInstance.getClusterName(), matchedInstance.getServiceName(), OBJECT_MAPPER.writeValueAsString(matchedInstance.getMetadata()), matchedInstance.getPort() + "@" + matchedInstance.getIp()};

                Object result = updateMethod.invoke(jdbcTemplate, sql, params);
                LOGGER.info("[loggingUpdateInstance] 更新数据库受影响行数: {}", result);

            } catch (NacosException | JsonProcessingException e) {
                LOGGER.error("[loggingUpdateInstance] Nacos/JSON exception:", e);
            } catch (Exception e) {
                LOGGER.error("[loggingUpdateInstance] JDBC invoke exception:", e);
            }
        }, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public List<Class<? extends TraceEvent>> subscribeTypes() {
        List<Class<? extends TraceEvent>> result = new LinkedList<>();
        result.add(RegisterInstanceTraceEvent.class);
        result.add(DeregisterInstanceTraceEvent.class);
        result.add(UpdateInstanceTraceEvent.class);
        return result;
    }
}
