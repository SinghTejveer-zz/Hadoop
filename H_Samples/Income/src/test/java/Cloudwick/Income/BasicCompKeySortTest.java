package Cloudwick.Income;

import static org.junit.Assert.*;

import java.io.IOException;
import org.junit.Test;
import Cloudwick.Income.CompositeKeyWrite;

public class BasicCompKeySortTest {

  @Test
  public void CompareTo_SmallToBig_MinusOne() throws IOException {

    // Arrange
    CompositeKeyWrite inp0 =
        new CompositeKeyWrite("Switzerland", 63318.3487591229);
    CompositeKeyWrite inp1 = new CompositeKeyWrite("Norway", 65636.7724056934);

    // Act
    int actual = inp0.compareTo(inp1);

    // Assert
    assertEquals(-1, actual);
  }

  @Test
  public void CompareTo_BigToSmall_One() throws IOException {

    // Arrange
    CompositeKeyWrite inp0 =
        new CompositeKeyWrite("Switzerland", 63318.3487591229);
    CompositeKeyWrite inp1 = new CompositeKeyWrite("Norway", 65636.7724056934);

    // Act
    int actual = inp1.compareTo(inp0);

    // Assert
    assertEquals(1, actual);
  }

  @Test
  public void CompareTo_Same_Zero() throws IOException {

    // Arrange
    CompositeKeyWrite inp0 = new CompositeKeyWrite("Switzerland", 123.123);
    CompositeKeyWrite inp1 = new CompositeKeyWrite("Norway", 123.123);

    // Act
    int actual = inp1.compareTo(inp0);

    // Assert
    assertEquals(0, actual);
  }

}