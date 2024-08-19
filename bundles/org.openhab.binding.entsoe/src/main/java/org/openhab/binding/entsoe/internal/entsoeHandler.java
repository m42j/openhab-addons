/**
 * Copyright (c) 2010-2024 Contributors to the openHAB project
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.openhab.binding.entsoe.internal;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.measure.Unit;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;
import org.openhab.binding.entsoe.internal.client.Client;
import org.openhab.binding.entsoe.internal.client.Request;
import org.openhab.binding.entsoe.internal.exception.entsoeConfigurationException;
import org.openhab.binding.entsoe.internal.exception.entsoeResponseException;
import org.openhab.binding.entsoe.internal.exception.entsoeResponseMapException;
import org.openhab.binding.entsoe.internal.exception.entsoeUnexpectedException;
import org.openhab.core.library.dimension.Currency;
import org.openhab.core.library.types.DateTimeType;
import org.openhab.core.library.types.DecimalType;
import org.openhab.core.library.types.QuantityType;
import org.openhab.core.library.unit.CurrencyUnit;
import org.openhab.core.library.unit.CurrencyUnits;
import org.openhab.core.thing.ChannelUID;
import org.openhab.core.thing.Thing;
import org.openhab.core.thing.ThingStatus;
import org.openhab.core.thing.ThingStatusDetail;
import org.openhab.core.thing.binding.BaseThingHandler;
import org.openhab.core.types.Command;
import org.openhab.core.types.RefreshType;
import org.openhab.core.types.State;
import org.openhab.core.types.TimeSeries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link entsoeHandler} is responsible for handling commands, which are
 * sent to one of the channels.
 *
 * @author Jørgen Melhus - Initial contribution
 */
@NonNullByDefault
public class entsoeHandler extends BaseThingHandler {

    private final Logger _logger = LoggerFactory.getLogger(entsoeHandler.class);

    private @Nullable entsoeConfiguration _config;

    private @Nullable ScheduledFuture<?> _refreshJob;

    private Unit<Currency> _baseUnit = CurrencyUnits.BASE_CURRENCY;
    private Unit<Currency> _fromUnit = new CurrencyUnit(entsoeBindingConstants.ENTSOE_CURRENCY, null);

    private @Nullable Map<ZonedDateTime, Double> _responseMap;

    public ZonedDateTime lastDayAheadReceived = ZonedDateTime.of(LocalDateTime.MIN, ZoneId.of("UTC"));

    public entsoeHandler(Thing thing) {
        super(thing);
    }

    @Override
    public void channelLinked(ChannelUID channelUID) {
        _logger.trace("channelLinked()");
        String channelID = channelUID.getId();
        _logger.info(String.format("Channel linked %s", channelID));

        if (channelID.equals(entsoeBindingConstants.CHANNEL_SPOT_PRICES))
            updateCurrentHourState(entsoeBindingConstants.CHANNEL_SPOT_PRICES);

        if (channelID.equals(entsoeBindingConstants.CHANNEL_LAST_DAY_AHEAD_RECEIVED) && lastDayAheadReceived
                .toEpochSecond() > ZonedDateTime.of(LocalDateTime.MIN, ZoneId.of("UTC")).toEpochSecond())
            updateState(entsoeBindingConstants.CHANNEL_LAST_DAY_AHEAD_RECEIVED, new DateTimeType(lastDayAheadReceived));
    }

    @Override
    public void dispose() {
        _logger.trace("dispose()");
        if (_refreshJob != null) {
            _refreshJob.cancel(true);
            _refreshJob = null;
        }
        super.dispose();
    }

    @Override
    public void handleCommand(ChannelUID channelUID, Command command) {
        _logger.trace("handleCommand()");
        _logger.debug("ChannelUID: {}, Command: {}", channelUID, command);

        if (command instanceof RefreshType) {
            _logger.debug("Command Instance Of Refresh");
            refreshPrices();
        } else {
            _logger.debug("Command Instance Not Implemented");
        }
    }

    @Override
    public void initialize() {
        _logger.trace("initialize()");
        updateStatus(ThingStatus.UNKNOWN);
        BigDecimal rate = getExchangeRate();
        if (rate.compareTo(new BigDecimal(0)) == 1) {
            updateStatus(ThingStatus.ONLINE);
            _logger.debug("Initialized %s", isInitialized());
            if (isInitialized())
                _refreshJob = scheduler.schedule(this::refreshPrices, 5, TimeUnit.SECONDS);
        }
    }

    private ZonedDateTime currentUtcTime() {
        return ZonedDateTime.now(ZoneId.of("UTC"));
    }

    private ZonedDateTime currentUtcTimeWholeHours() {
        return ZonedDateTime.now(ZoneId.of("UTC")).truncatedTo(ChronoUnit.HOURS);
    }

    private BigDecimal getCurrentHourExchangedKwhPrice() throws entsoeResponseMapException {
        _logger.trace("getCurrentHourExchangedKwhPrice()");
        if (_responseMap == null) {
            throw new entsoeResponseMapException("responseMap is empty");
        }
        Integer currentHour = currentUtcTime().getHour();
        Integer currentDayInYear = currentUtcTime().getDayOfYear();
        var result = _responseMap.entrySet().stream()
                .filter(x -> x.getKey().getHour() == currentHour && x.getKey().getDayOfYear() == currentDayInYear)
                .findFirst();
        if (result.isEmpty()) {
            throw new entsoeResponseMapException(
                    String.format("Could not find a value for hour %s and day in year %s in responseMap", currentHour,
                            currentDayInYear));
        }

        double eurMwhPrice = result.get().getValue();
        BigDecimal exchangedKwhPrice = getExchangedKwhPrice(eurMwhPrice);
        return exchangedKwhPrice;
    }

    private BigDecimal getExchangedKwhPrice(double eurMwhPrice) {
        _logger.trace("getExchangedKwhPrice()");
        BigDecimal vat = BigDecimal.valueOf((_config.vat + 100.0) / 100.0);
        BigDecimal mwhPrice = vat.multiply(BigDecimal.valueOf(eurMwhPrice));
        BigDecimal kwhPrice = mwhPrice.divide(new BigDecimal(1000), 20, RoundingMode.HALF_UP);
        BigDecimal exchangeRate = getExchangeRate();
        BigDecimal exchangedKwhPrice = kwhPrice.divide(exchangeRate, 10, RoundingMode.HALF_UP);

        return exchangedKwhPrice;
    }

    private BigDecimal getExchangeRate() {
        _logger.trace("getExchangeRate()");
        BigDecimal rate = CurrencyUnits.getExchangeRate(_fromUnit);

        if (rate == null) {
            rate = new BigDecimal(0);
            String msg = "Could not get exchange rate. Have you configured a currency binding to fetch currency rates and set your base currency?";
            _logger.error(msg);
            updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.CONFIGURATION_ERROR, msg);
        } else {
            _logger.debug("Exchange rate {}{}: {}", _fromUnit.getName(), _baseUnit.getName(), rate.floatValue());
        }
        return rate;
    }

    private void refreshPrices() {
        _logger.trace("refreshPrices()");
        if (!isLinked(entsoeBindingConstants.CHANNEL_SPOT_PRICES)) {
            _logger.debug("Channel {} is not linked, cant update channel", entsoeBindingConstants.CHANNEL_SPOT_PRICES);
            return;
        }

        _config = getConfigAs(entsoeConfiguration.class);

        ZonedDateTime startUtc = currentUtcTimeWholeHours().minusDays(_config.historicDays).withHour(22);
        ZonedDateTime endUtc = currentUtcTimeWholeHours().plusDays(1).withHour(22);

        boolean needsUpdate = lastDayAheadReceived == ZonedDateTime.of(LocalDateTime.MIN, ZoneId.of("UTC"))
                || _responseMap == null;
        boolean hasNextDayValue = needsUpdate ? false
                : _responseMap.entrySet().stream()
                        .anyMatch(x -> x.getKey().getDayOfYear() == currentUtcTime().plusDays(1).getDayOfYear());
        boolean readyForNextDayValue = needsUpdate ? true
                : currentUtcTime().toEpochSecond() > currentUtcTime().withHour(_config.spotPricesAvailableUtcHour)
                        .toEpochSecond();

        // Update whole time series
        if (needsUpdate || (!hasNextDayValue && readyForNextDayValue)) {
            _logger.debug("Updating timeseries");
            Request request = new Request(_config.securityToken, _config.area, startUtc, endUtc);
            Client client = new Client();
            boolean success = false;

            try {
                _responseMap = client.doGetRequest(request, 10000);
                TimeSeries baseTimeSeries = new TimeSeries(entsoeBindingConstants.TIMESERIES_POLICY);
                for (Map.Entry<ZonedDateTime, Double> entry : _responseMap.entrySet()) {
                    BigDecimal exchangedKwhPrice = getExchangedKwhPrice(entry.getValue());
                    State baseState = new QuantityType<>(exchangedKwhPrice + " " + _baseUnit.getName() + "/kWh");
                    baseTimeSeries.add(entry.getKey().toInstant(), baseState);
                }
                lastDayAheadReceived = currentUtcTime();
                sendTimeSeries(entsoeBindingConstants.CHANNEL_SPOT_PRICES, baseTimeSeries);
                updateState(entsoeBindingConstants.CHANNEL_LAST_DAY_AHEAD_RECEIVED,
                        new DateTimeType(lastDayAheadReceived));
                updateCurrentHourState(entsoeBindingConstants.CHANNEL_SPOT_PRICES);
                triggerChannel(entsoeBindingConstants.CHANNEL_TRIGGER_PRICES_RECEIVED);
                success = true;
            } catch (entsoeResponseException e) {
                updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.COMMUNICATION_ERROR,
                        String.format("%s", e.getMessage()));
                _logger.error("{}", e.getMessage());
            } catch (entsoeUnexpectedException e) {
                updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.COMMUNICATION_ERROR,
                        String.format("%s", e.getMessage()));
                _logger.error("{}", e.getMessage());
            } catch (entsoeConfigurationException e) {
                updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.CONFIGURATION_ERROR,
                        String.format("%s", e.getMessage()));
                _logger.error("{}", e.getMessage());
            } finally {
                schedule(success);
            }
        }
        // Update current hour with refreshed exchange rate
        else {
            _logger.debug("Updating current hour");
            updateCurrentHourState(entsoeBindingConstants.CHANNEL_SPOT_PRICES);
            schedule(true);
        }
    }

    private int getSecondsToNextHour(int additionalWaitMinutes) {
        _logger.trace("getSecondsToNextHour()");
        int minutesToNextHour = 60 - currentUtcTime().getMinute();
        int seconds = (minutesToNextHour + additionalWaitMinutes) * 60;
        return seconds;
    }

    private void schedule(boolean success) {
        _logger.trace("schedule()");
        if (!success) {
            // not successful, run again in 5 minutes
            _refreshJob = scheduler.schedule(this::refreshPrices, 300, TimeUnit.SECONDS);
        } else {
            // run each whole hour
            _refreshJob = scheduler.schedule(this::refreshPrices, getSecondsToNextHour(1), TimeUnit.SECONDS);
        }
    }

    private void updateCurrentHourState(String channelID) {
        _logger.trace("updateCurrentHourState()");
        try {
            BigDecimal exchangedKwhPrice = getCurrentHourExchangedKwhPrice();
            updateState(channelID, new DecimalType(exchangedKwhPrice));
        } catch (entsoeResponseMapException e) {
            updateStatus(ThingStatus.OFFLINE, ThingStatusDetail.CONFIGURATION_ERROR,
                    String.format("%s", e.getMessage()));
            _logger.error("{}", e.getMessage());
        }
    }
}
