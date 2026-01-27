import asyncio
import logging
from pylabrobot.barcode_scanners.backend import (
  BarcodeScannerBackend,
  BarcodeScannerError,
)

import serial
import time
import threading

from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Optional
from threading import Event
from pylabrobot.io.serial import Serial

logger = logging.getLogger(__name__)

class KeyenceBarcodeScannerBackend(BarcodeScannerBackend):
  default_baudrate = 9600
  serial_messaging_encoding = "ascii"
  init_timeout = 1.0  # seconds
  poll_interval = 0.2  # seconds

  def __init__(self, serial_port: str,):
    super().__init__()

    # BL-1300 Barcode reader factory default serial communication settings
    # should be the same factory default for the BL-600HA and BL-1300 models
    self.io = Serial(
      port=serial_port,
      baudrate=self.default_baudrate,
      bytesize=serial.SEVENBITS,
      parity=serial.PARITY_EVEN,
      stopbits=serial.STOPBITS_ONE,
      write_timeout=1,
      timeout=1,
      rtscts=False,
    )

    # buffer for incoming bytes
    self._buffer = bytearray()
    self._recv_callback: Optional[Callable[[str], None]] = None

    # internal response synchronization for send_command
    self._response_event = asyncio.Event()
    self._response_lock = threading.Lock()
    self._pending_slots: deque[_ResponseSlot] = deque()
    self._unsolicited_condition = threading.Condition(self._response_lock)
    self._active_slot: Optional[_ResponseSlot] = None
    self._unsolicited_messages: deque[str] = deque()

    # reader task
    self._reader_task: Optional[asyncio.Task] = None
    self._stop_event = asyncio.Event()

  async def setup(self):
    await self.io.setup()
    # Start the reader loop as an async task
    self._reader_task = asyncio.create_task(self._reader_loop())
    await self.initialize_scanner()

  async def initialize_scanner(self):
    """Initialize the Keyence barcode scanner."""

    response = await self.send_command("RMOTOR")

    deadline = time.time() + self.init_timeout
    while time.time() < deadline:
      response = await self.send_command("RMOTOR")
      if response and response.strip() == "MOTORON":
        print("Barcode scanner motor is ON.")
        break
      elif response and response.strip() == "MOTOROFF":
        raise BarcodeScannerError("Failed to initialize Keyence barcode scanner: Motor is off.")
      await asyncio.sleep(self.poll_interval)
    else:
      raise BarcodeScannerError("Failed to initialize Keyence barcode scanner: " \
      "Timeout waiting for motor to turn on.")

  async def _reader_loop(self) -> None:
    """Async background reader that collects bytes and emits messages on CR delimiter."""
    plc_delim = b"\r"

    try:
      while not self._stop_event.is_set():
        try:
          # Async read from serial
          data = await self.io.read(256)
        except Exception as e:
          logger.error(f"Error reading from serial: {e}")
          await asyncio.sleep(0.1)
          continue

        if data:
          self._buffer.extend(data)
          # extract all messages terminated by CR
          while True:
            idx = bytes(self._buffer).find(plc_delim)
            if idx == -1:
              break
            raw = bytes(self._buffer[:idx])
            del self._buffer[:idx + len(plc_delim)]

            # If delimiter is CR only and the sender actually sent CRLF,
            # remove a leftover LF (0x0A) that may start the next buffer.
            if plc_delim == b"\r" and len(self._buffer) > 0 and self._buffer[0] == 0x0A:
              del self._buffer[0]

            try:
              text = raw.decode('ascii', errors='replace')
            except Exception:
              text = raw.decode('utf-8', errors='replace')

            # Route to appropriate handler
            slot = self._active_slot
            if slot is None and self._pending_slots:
              slot = self._pending_slots.popleft()
              self._active_slot = slot

            if slot is not None and slot.cancelled:
              self._active_slot = None
              slot = None

            if slot is not None:
              slot.responses.append(text)
              if len(slot.responses) >= slot.expected_responses:
                self._active_slot = None
                self._response_event.set()
            else:
              self._unsolicited_messages.append(text)

            # call user callback if present
            if self._recv_callback:
              try:
                self._recv_callback(text)
              except Exception as e:
                logger.error(f"Error in receive callback: {e}")
        else:
          # no data; yield to event loop
          await asyncio.sleep(0.01)
    except asyncio.CancelledError:
      pass
    except Exception as e:
      logger.error(f"Reader loop error: {e}")

  async def _send_command_internal(
      self,
      text: str,
      timeout: Optional[float] = None,
      expected_responses: int = 1,
  ) -> Optional[str]:
    """Send a command and wait for response (async version)."""

    if expected_responses < 1:
      raise ValueError("expected_responses must be >= 1")

    slot = _ResponseSlot(expected_responses=expected_responses)
    self._pending_slots.append(slot)

    # Send the command
    await self.send_text(text)

    # Wait for response
    try:
      self._response_event.clear()
      await asyncio.wait_for(self._wait_for_slot(slot), timeout=timeout)
      return slot.responses[0] if slot.responses else None
    except asyncio.TimeoutError:
      slot.cancelled = True
      if self._active_slot is slot:
        self._active_slot = None
      else:
        try:
          self._pending_slots.remove(slot)
        except ValueError:
          pass
      return None

  async def _wait_for_slot(self, slot: '_ResponseSlot') -> None:
    """Wait for a response slot to be filled."""
    while not slot.event.is_set():
      await asyncio.sleep(0.01)

  def set_receive_callback(self, callback: Callable[[str], None]):
    """Register a callback invoked with each received ASCII message."""
    self._recv_callback = callback

  async def send_text(self, text: str) -> None:
    """Send an ASCII text command; append CR delimiter."""
    if not self.io:
      raise serial.SerialException("Serial port not open")

    payload = text.encode('ascii', errors='replace') + b"\r"
    await self.io.write(payload)

  async def send_command(
      self,
      command: str,
      timeout: Optional[float] = None,
      expected_responses: int = 1
  ) -> Optional[str]:
    """Send a command to the barcode scanner and return the response."""
    return await self._send_command_internal(command, timeout=timeout, expected_responses=expected_responses)

  def wait_for_response(self, timeout: Optional[float] = None) -> Optional[str]:
    """
    Block the calling thread until the next PLC response is received.

    Does NOT send a command — it only waits for the next incoming message.

    If timeout is None: wait indefinitely.
    If timeout is a float: wait up to that many seconds.

    Returns:
        str: the received ASCII response
        None: if timed out
    """

    with self._unsolicited_condition:
        if not self._unsolicited_messages:
            try:
                fired = self._unsolicited_condition.wait(timeout)
            except Exception:
                fired = False
            if not fired:
                return None
        if not self._unsolicited_messages:
            return None
        return self._unsolicited_messages.popleft()

  async def stop(self):
    self._stop_event.set()
    if self._reader_task:
      try:
        await asyncio.wait_for(self._reader_task, timeout=1.0)
      except asyncio.TimeoutError:
        self._reader_task.cancel()
    await self.io.stop()

  async def scan_barcode(self) -> str:
    result = await self.send_command("LON")
    return result if result else ""


@dataclass
class _ResponseSlot:
  expected_responses: int
  responses: list[str] = field(default_factory=list)
  event: Event = field(default_factory=Event)
  cancelled: bool = False
