package com.softwareverde.bitcoin.block.header.difficulty;

import com.softwareverde.bitcoin.type.hash.Hash;
import com.softwareverde.bitcoin.util.ByteUtil;
import com.softwareverde.constable.Const;

import java.math.BigDecimal;
import java.math.BigInteger;

public class ImmutableDifficulty implements Difficulty, Const {
    private final Integer _exponent;
    private final byte[] _significand = new byte[3];

    private byte[] _cachedBytes = null;

    public static ImmutableDifficulty decode(final byte[] encodedBytes) {
        if (encodedBytes.length != 4) { return null; }
        return new ImmutableDifficulty(ByteUtil.copyBytes(encodedBytes, 1, 3), (ByteUtil.byteToInteger(encodedBytes[0]) - 3));
    }

    protected BigDecimal _toBigDecimal() {
        return new BigDecimal(new BigInteger(_convertToBytes()), 4);
    }

    public ImmutableDifficulty(final byte[] significand, final Integer exponent) {
        _exponent = exponent;

        final int copyCount = Math.min(_significand.length, significand.length);
        for (int i=0; i<copyCount; ++i) {
            _significand[(_significand.length - i) - 1] = significand[(significand.length - i) - 1];
        }
    }

    public ImmutableDifficulty(final Difficulty difficulty) {
        _exponent = difficulty.getExponent();
        ByteUtil.setBytes(_significand, difficulty.getSignificand());
    }

    protected byte[] _convertToBytes() {
        final byte[] bytes = new byte[32];
        ByteUtil.setBytes(bytes, _significand, (32 - _exponent - _significand.length));
        return bytes;
    }

    @Override
    public byte[] getBytes() {
        return _convertToBytes();
    }

    @Override
    public byte[] encode() {
        final byte[] bytes = new byte[4];
        bytes[0] = (byte) (_exponent + 3);
        ByteUtil.setBytes(bytes, _significand, 1);
        return bytes;
    }

    @Override
    public Integer getExponent() { return _exponent; }

    @Override
    public byte[] getSignificand() { return ByteUtil.copyBytes(_significand); }

    @Override
    public Boolean isSatisfiedBy(final Hash hash) {
        if (_cachedBytes == null) {
            _cachedBytes = _convertToBytes();
        }

        for (int i=0; i<_cachedBytes.length; ++i) {
            // if (i > 2) Logger.log(HexUtil.toHexString(_cachedBytes) + " " + hash);
            final int difficultyByte = ByteUtil.byteToInteger(_cachedBytes[i]);
            final int sha256Byte = ByteUtil.byteToInteger(hash.getByte(i));
            if (sha256Byte == difficultyByte) { continue; }
            return (sha256Byte < difficultyByte);
        }

        return true;
    }

    @Override
    public BigDecimal getDifficultyRatio() {
        final BigDecimal currentValue = _toBigDecimal();
        final BigDecimal baseDifficultyValue = Difficulty.BASE_DIFFICULTY._toBigDecimal();
        return baseDifficultyValue.divide(currentValue, BigDecimal.ROUND_HALF_UP);
    }

    @Override
    public Difficulty multiplyBy(final float difficultyAdjustment) {
        final BigDecimal currentValue = _toBigDecimal();
        final BigDecimal bigDecimal = currentValue.multiply(BigDecimal.valueOf(difficultyAdjustment));
        return new ImmutableDifficulty(bigDecimal.unscaledValue().toByteArray(), bigDecimal.scale());
    }

    @Override
    public ImmutableDifficulty asConst() {
        return this;
    }

    @Override
    public boolean equals(final Object object) {
        if (object == null) { return false; }
        if (! (object instanceof Difficulty)) { return false; }

        final Difficulty difficulty = (Difficulty) object;
        if (! _exponent.equals(difficulty.getExponent())) { return false; }

        return ByteUtil.areEqual(_significand, difficulty.getSignificand());
    }
}
